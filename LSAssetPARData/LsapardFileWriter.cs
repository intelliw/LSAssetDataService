using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LSAssetDataService {

    /* Extends a FileWriter for the asset PAR data file */
    internal class LsapardFileWriter : FileWriter {

        #region VARIABLES & CONSTANTS --------------------------------------------------------------

        private const FileTypeEnum DATAFileType = FileTypeEnum.XLSX;                        // e.g. AssetPARDataFile_2017_08_10_10_01.xlsx

        private const string DATAFilenamePrefix = "AssetPARDataFile";                       // e.g. AssetPARDataFile_2017_08_10_10_01.xlsx
        private const string ARCHIVESubFolder = "Archive";                                  // folder must exist (use script 'LSAssetDataService-Registry and Folders.ps1')
        private const int FILESToRetainWhenArchiving = 3;                                   // retain this nubmer of files in the output folder, archive the rest

        #endregion

        #region INITIALISATION ---------------------------------------------------------------------

        // default constructor 
        public LsapardFileWriter(string outputFolder, EventLogger logger) : base(outputFolder, logger) {
            // nothing to do here - all taken care of in base
        }

        #endregion
        
        #region OVERRIDEN - methods overridden from LSAssetData ------------------------------------

        // the prefix for the output filename
        internal sealed override string DataFilenamePrefix => DATAFilenamePrefix;

        // the prefix for the output filename
        internal sealed override FileTypeEnum DataFileType => DATAFileType;

        // archive folder - must exist (create using script 'LSAssetDataService-Registry.ps1') 
        internal override string ArchiveFolderPath => base.OutputFolderPath + "\\" + ARCHIVESubFolder;

        // the number of files to retain - the newest files are retained in the output folder, the rest are archived
        internal override int FilesToRetainWhenArchiving => FILESToRetainWhenArchiving;


        /* creates a file with the headers and data needed for output PAR equipment level report 
         * and writes out a file depending on the DataFileType of this service subclass - either a CSV or XLSX - . 
         * depending on the DataFileType of this service . The created filename is timestamped based on the last change
         * date in AssetDataRecordset.LastModified. The returned asset recordset will have zero rows if there are no changed 
         * records. This method is invoked in the subclass by the service event (e.g LSAsetPARData.OnService).
         * The subclass executes service specific functions then invokes the super to write out the recordset to disk
         * The LSapard output file structure is : 
         *  | PAR Rule| PAR Rule-Status | PAR Rule-Qty | PAR Rule-Repl Qty | PAR Rule-Date | Cur-Status | Cur-Qty | Cur-Repl Qty | Cur-Status Date 
                | Level | Zone | Zone-Type | Asset-Category | Asset-Type | Asset-Model | Asset-Model Descr | Workflow Statuses | PAR Rule-ID | Asset-Model/Type ID | Zone-ID
         * Unlike other asset data file exports this output file does NOT append the database column names from the query result.
         */
        internal sealed override AssetDataRecordset WriteFile(AssetDataRecordset recordset, string targetFileSpec = "", bool overwrite = false) {

            string[] ParTabHeaders = { "Cur-Status", "Cur-Qty", "Cur-Repl Qty", "PAR Rule-Status", "PAR Rule-Qty", "PAR Rule-Repl Qty",
                "Level", "Zone-Type", "Zone", "Asset-Category", "Asset-Type", "Asset-Model", "Asset-Model Descr", "Workflow Statuses",
                "PAR Rule-Name", "PAR Rule-Date", "Cur-Status Date"}; 

            const string ABOVEParStatus = "Above PAR";                                                          // status label in CORE tbEnum
            const string ATParStatus = "At PAR";                                                                // status label in CORE tbEnum
            const string BELOWParStatus = "Below PAR";                                                          // status label in CORE tbEnum
            const string NOParStatus = "";                                                                      // status when PAR is reset 

            const string MASTERFileName = "MASTER_AssetPARDataFile";                                            // the name of the master file. This file is overwritten each time a new AssetPARDataFile is cretaed in the target folder.

            const string TAB1Name = "PAR Data";                                                                 // the name of the first tab

            // create the output recordset                     
            AssetDataRecordset targetData = new AssetDataRecordset();
            targetData.LastModified = recordset.LastModified;                                                   // initialise with source recordset's cutoff time

            // add the file headers and data rows into the  targetData output recordset                  
            try {

                // COLUMNS -------------------------------------------------------------------                  
                targetData.Columns = new List<string>(ParTabHeaders);                                           // add the Equipment Level Management Report headers 

                // ROWS ---------------------------------------------------------------------                   // add escaped data for each row
                foreach (AssetDataRow sourceRow in recordset.Rows) {
                    AssetDataRow newRow = new AssetDataRow();

                    // row variables                    
                    int curReplQty = 0;
                    int curQty, parRuleQty, parRuleReplQty;                                                     // replenishment quantity depends on current qty, PAR Rule qty, and PAR Rule qty repl qty
                    string parRuleStatus = sourceRow.Fields[recordset.Index.ParRuleStatus];
                    string curStatus = sourceRow.Fields[recordset.Index.AssetOrParStatus];                      // initialise from CORE query - but overwrite below to workaround 1) lagging updates in CORE and 2) inconsistent current state in CORE when a rule's PAR Status is reconfigured 
                    string curStatusDate = sourceRow.Fields[recordset.Index.LastChanged];                       // read the date as a string 

                    Int32.TryParse(sourceRow.Fields[recordset.Index.AssetQuantity], out curQty);
                    Int32.TryParse(sourceRow.Fields[recordset.Index.ParRuleQty], out parRuleQty);
                    Int32.TryParse(sourceRow.Fields[recordset.Index.ParRuleRepQty], out parRuleReplQty);

                    DateTime rowLastChanged = targetData.LastModified;                                          // initialise, replace with row's lastchanged next        

                    // calculate the current replenishment quantity for this row                                // the replenishment qty calculates qty needed to pick up or drop off so that equipment quantities are set to level at which PAR Status would get reset (regardless of whether the current PAR status is set) 
                    switch (parRuleStatus) {
                        case ABOVEParStatus:
                            if (curQty > (parRuleQty - parRuleReplQty)) {                                       // do not recommend pickup if current quantity is already low, only if it is higher than optimum level determined by parRuleQty - parRuleReplQty
                                curReplQty = (parRuleQty - parRuleReplQty) - curQty;                            // for Above PAR rules the replenishment is negative (pick up): based on (PAR Rule Qty - PAR Rule Repl Qty) - current qty
                            }
                            break;
                        case BELOWParStatus:                                                                    // do not recommend drop off if current quantity is already high, only if it is lower than optimum level determined by parRuleQty + parRuleReplQty
                            if (curQty < (parRuleQty + parRuleReplQty)) {
                                curReplQty = (parRuleQty + parRuleReplQty) - curQty;                            // for Below PAR rules the replenishment qty is positive (drop off): based on (PAR Rule Qty + PAR Rule Repl Qty) - current qty
                            }
                            break;
                        case ATParStatus:                                                                       // for At PAR rules 
                            if (curQty >= parRuleQty) {                                                         // if the current qty is greater than the rule qty     
                                curReplQty = (parRuleQty + parRuleReplQty) - curQty;                            // status is reset if count goes to greater than or equal to parRuleQty + parRuleReplQty
                            } else {                                                                            // if the current qty is less than the rule qty     
                                curReplQty = (parRuleQty - parRuleReplQty) - curQty;                            // status is reset if count goes to less than or equal to parRuleQty - parRuleReplQty            
                            }
                            break;
                    }

                    // SET current status - check if the current status is not blank and different to the par rule status, of so it should be set or reset to blank - as there is a defect in CORE which shows an incorrect status when a par rule's status is changed after a current status has been set previously 
                    if (String.IsNullOrEmpty(curStatus) || (!String.IsNullOrEmpty(curStatus) && (curStatus != parRuleStatus))) {      // set if curstatus empty, or if current status in CORE does not match the rule status
                        if ((parRuleStatus == BELOWParStatus) && (curQty < parRuleQty)) {                       // must be less than (NOT equal to)
                            curStatus = BELOWParStatus;
                        } else if ((parRuleStatus == ABOVEParStatus) && (curQty > parRuleQty)) {
                            curStatus = ABOVEParStatus;
                        } else if ((parRuleStatus == ATParStatus) && (curQty == parRuleQty)) {
                            curStatus = ATParStatus;
                        } else {
                            curStatus = NOParStatus;
                        }
                    // RESET current status - if the status is currently set check if it needs to be reset (as CORE is very slow to do this, and the report will show quantities which contradict stauses until CORE does its updates)
                    } else if (!String.IsNullOrEmpty(curStatus) && (curStatus == parRuleStatus))  {             // check reset if current status is set 
                        if ((parRuleStatus == BELOWParStatus) && (curQty >= (parRuleQty + parRuleReplQty))) {   // must be greater than OR equal to          
                            curStatus = NOParStatus;
                        } else if ((parRuleStatus == ABOVEParStatus) && (curQty <= (parRuleQty - parRuleReplQty))) {   
                            curStatus = NOParStatus;
                        } else if ((parRuleStatus == ATParStatus) && (curQty <= (parRuleQty - parRuleReplQty) || curQty >= (parRuleQty + parRuleReplQty))) {
                            curStatus = NOParStatus;
                        }
                    }

                    // track the last changed par status datetime                                               // for this service track last changed based on par status LastChanged field
                    if (!String.IsNullOrEmpty(curStatusDate)) {                                                 // skip rows which do not refer to a par rule, these rows are for asset counts but do not have a curStatusDate 
                        if (!(DateTime.TryParse(curStatusDate, out rowLastChanged))) {                          // check if the date can be parsed - it may be null if a par status had never been set for this par rule row
                            rowLastChanged = targetData.LastModified;                                               // if the data field could not be parsed keep the previous date time unchanged
                                                                                                                Log.Trace("Could not parse row LastChanged.. (" + curStatusDate + ")", EventLogger.EventIdEnum.QUERYING_DATA);
                        }
                    }

                    // add the row - add data for each ELMR COLUMN 
                    newRow.Fields.Add(EscapeCharacters(curStatus));                                             // Cur-Status
                    newRow.Fields.Add(curQty.ToString());                                                       // Cur-Qty
                    newRow.Fields.Add(curReplQty.ToString());                                                   // Cur-Repl Qty    

                    newRow.Fields.Add(EscapeCharacters(parRuleStatus));                                         // PAR Rule-Status 
                    newRow.Fields.Add(parRuleQty.ToString());                                                   // PAR Rule-Qty
                    newRow.Fields.Add(parRuleReplQty.ToString());                                               // PAR Rule-Repl Qty    

                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.Level]));               // Level 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.ZoneType]));            // Zone-Type 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetSublocationOrZone]));  // Zone 

                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetCategory]));       // Asset-Category 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetType]));           // Asset-Type 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetModel]));          // Asset-Model 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetModelDescription]));   // Asset-Model Descr
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.WorkflowStatus]));      // 17. Workflow Statuses 

                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.ParRule]));             // 14. PAR Rule 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.CoreModifiedDate]));    // 15. PAR Rule-Date     
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.LastChanged]));         // 16. Cur-Status Date 

                    // keep a tab on the latest change to use in the output file 
                    if (rowLastChanged > targetData.LastModified) { targetData.LastModified = rowLastChanged; }
                    
                    // add the row to the recordset
                    targetData.Rows.Add(newRow);

                }
                //set the name of the first tab
                targetData.Name = TAB1Name;

                // NOTE USED - add supplementary data
                // targetData.SupplementaryData = AddSupplementaryData(recordset.SupplementaryData);

                // WRITE FILE - write to disk in base class
                targetData = base.WriteFile(targetData, overwrite:true);                                        // write with overwrite. Returns lastchanged in targetData recordset

                // OVERWRITE and UPDATE the MASTER FILE                                                                        
                if (targetData.Saved) {
                    string masterFileSpec = Path.Combine(OutputFolderPath, MASTERFileName + "." + DataFileExtension);
                    targetData = base.WriteFile(targetData, masterFileSpec, true);
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return targetData;
        }

        // NOTE USED - adds supplementary data fdor the second tab
        private AssetDataRecordset AddSupplementaryData(AssetDataRecordset recordset) {
            string[] AssetCountTabHeaders = { "Cur-Qty", "Level", "Zone-Type", "Zone", "Asset-Category", "Asset-Type", "Asset-Model", "Workflow Statuses" };

            const string TAB2Name = "PAR Count";                                                                 // the name of the second tab

            // create the output recordset                     
            AssetDataRecordset targetData = new AssetDataRecordset();
            targetData.LastModified = recordset.LastModified;                                                   // initialise with source recordset's cutoff time

            // add the file headers and data rows into the  targetData output recordset                  
            try {

                // COLUMNS -------------------------------------------------------------------                  
                targetData.Columns = new List<string>(AssetCountTabHeaders);                                    // add the Equipment Level Management Report headers 

                // ROWS ---------------------------------------------------------------------                   // add escaped data for each row
                foreach (AssetDataRow sourceRow in recordset.Rows) {
                    AssetDataRow newRow = new AssetDataRow();

                    // add the row - add data for each ELMR COLUMN 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetQuantity]));       // 01. Cur-Qty
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.Level]));               // 02. Level 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.ZoneType]));            // 03. Zone-Type 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetSublocationOrZone]));  // 04. Zone 

                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetCategory]));       // 05. Asset-Category 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetType]));           // 06. Asset-Type 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetModel]));          // 07. Asset-Model 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.WorkflowStatus]));      // 08. Workflow Statuses 

                    // add the row to the recordset
                    targetData.Rows.Add(newRow);

                }

                //set the name of the second tab
                targetData.Name = TAB2Name;

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return targetData;

        }

        #endregion
    }
}
