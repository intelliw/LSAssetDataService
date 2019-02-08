using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LSAssetDataService {

    /* Extends a FileWriter for the asset workflow data file */
    internal class LsawdFileWriter : FileWriter {

        #region VARIABLES & CONSTANTS --------------------------------------------------------------

        private const FileTypeEnum DATAFileType = FileTypeEnum.CSV;                         // e.g. AssetDataFile_2017_08_10_10_01.csv

        private const string DATAFilenamePrefix = "AssetDataFile";                          // e.g. AssetDataFile_2017_08_10_10_01.csv
        private const string ARCHIVESubFolder = "Archive";                                  // folder must exist (use script 'LSAssetDataService-Registry and Folders.ps1')
        private const int FILESToRetainWhenArchiving = 3;                                   // retain this nubmer of files in the output folder, archive the rest

        #endregion

        #region INITIALISATION ---------------------------------------------------------------------

        // default constructor 
        public LsawdFileWriter(string outputFolder, EventLogger logger) : base(outputFolder, logger) {
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
        
        /* create an asset data recordset with the headers and data needed for output
         * and writes out a file depending on the DataFileType of this service subclass - either a CSV or XLSX - . 
         * depending on the DataFileType of this service . The created filename is timestamped based on the last change
         * date in AssetDataRecordset.LastModified. The returned asset recordset will have zero rows if there are no changed 
         * records. This method is invoked in the subclass by the service event (e.g LSAsetWorkflowData.OnService).
         * The subclass executes service specific functions then invokes the super to write out the recordset to disk
         * The LSAWD  output file structure is : 
         *  | Asset Category | Asset Type | Asset Model | Asset Code | Asset Status | Last Modified |
         * In addition the output file appends the database column names from thje query result, to help with 
         * data-related troubleshooting and analysis at runtime 
         * In addition the output file appends the following column to show the time since cutoff, for any records which were changed 
         * before the current period
         *  | __Before Cutoff | 
         */
        internal sealed override AssetDataRecordset WriteFile(AssetDataRecordset recordset, string targetFileSpec = "", bool overwrite = false) {

            string[] COREFileHeaders = { "Asset Category", "Asset Type", "Asset Model", "Asset Code", "Asset Status", "Last Modified" };
            const string retroActiveCutoffHeader = "__Changed Before Cutoff";                                   // double underscore prefix for special columns 
            const string DBColumnPrefix = "_";                                                                  // prefix database columns in the output file with an underscore to make these appear distinct from columns which CORE consumes

            const string TAB1Name = "Workflow Data";                                                             // the name of the first tab
            const string MINUTEPrecisionDateTimeFormat = "dd/MM/yyyy HH:mm";                                         
            
            // create the output recordset                     
            AssetDataRecordset targetData = new AssetDataRecordset();
            targetData.LastModified = recordset.LastModified;                                                   // initialise with source recordset's cutoff time

            // add the file headers and data rows into the targetData output recordset                  
            try {

                // COLUMNS -------------------------------------------------------------------                  // add specified columns to comply with the output file specifation, and database columns from the source recordset
                targetData.Columns = new List<string>(COREFileHeaders);                                         // CORE COLUMNS - first add the service-specific headers as specified

                targetData.Columns.Add(EscapeCharacters(retroActiveCutoffHeader));                              // SUPPLEMENTARY COLUMNS - add "__Changed Before Cutoff" to show which rows were retroactively included in the results 

                foreach (string column in recordset.Columns) {                                                  // DB COLUMN HEADERS - append the database column names at the end, prefix each with an underscore to make these distinct from the columns which CORE consumes
                    targetData.Columns.Add(EscapeCharacters(DBColumnPrefix + column));                          
                }

                // ROWS ---------------------------------------------------------------------                   // add escaped data for each row
                foreach (AssetDataRow sourceRow in recordset.Rows) {

                    AssetDataRow newRow = new AssetDataRow();

                    DateTime agilityLastChanged = DateTime.Parse(sourceRow.Fields[recordset.Index.LastChanged]);    // by default track last changed based on Agility LastChange field
                    DateTime coreLastChanged = DateTime.Parse(sourceRow.Fields[recordset.Index.CoreModifiedDate]);  // use this to track assets which have just been provisioned in CORE and have a null workflow status, the CORE modified date is used to timestamp these record instead of the agility lastmodified 
                    string coreStatus = sourceRow.Fields[recordset.Index.CoreWorkflowStatus].Trim();                // check the CORE workflow status as this needs to be inspected to implement a workaround for defect 2894 (failing workflowstatus updates) see ALM SAD

                    // check if this is a newly provisioned asset (core status is empty), and if so replace the agility last changed date time with the core last modified. This implements a workaround for defect 2894 (failing workflowstatus updates) see ALM SAD
                    DateTime lastChanged = agilityLastChanged;                                                  // by default the last changed timestamp for the row is based on agility    
                    if (String.IsNullOrEmpty(coreStatus)) {                                                     // if core status is null it means the asset has just been provisioned and CORE has not yet imported a workflow status: the coreStatus will be null and Last Modified should be set to the core modified date and time  
                        lastChanged = coreLastChanged;
                    }

                    // keep a tab on the latest change to use in the output file 
                    if (lastChanged > targetData.LastModified) { targetData.LastModified = lastChanged;}

                    // if the row timestamp is later than the file timestamp then round down lastChanged to the nearest mniute , this is required to implement the second workaround for defect 2894 (failing workflowstatus updates) see ALM SAD
                    if (lastChanged >= targetData.LastModified) {
                        lastChanged = Convert.ToDateTime(lastChanged.ToString(MINUTEPrecisionDateTimeFormat)).AddSeconds(-1);   // round down to the minute and subtract a second
                    }

                    // add the CORE-specified  file fields ----------------------------------------             // CORE COLUMNS - first add the service-specific headers as specified by CORE 
                    newRow.Fields = new List<string>(new string[] { "", "", "" });                              // "Asset Category", "Asset Type", "Asset Model" - column 1 - 3, empty - unused but CORE needs these - known defect
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetCode]));           // "Asset Code" - column 4 
                    newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetOrParStatus]));    // "Asset Status" - column 5  
                    newRow.Fields.Add(EscapeCharacters(lastChanged.ToString(ISO8601DateTimeFormat)));           // "Last Modified" - column 6 (LastChanged) e.g. '11/10/2017 16:46:00.507' 

                    // now append the supplementary fields ----------------------------------                   // SUPPLEMENTARY COLUMNS - double underscore prefix for special columns 
                    string timeSinceCutoff = "";
                    if (agilityLastChanged < recordset.LastModified) {
                        timeSinceCutoff = (recordset.LastModified.Subtract(agilityLastChanged)).ToString(@"hh\:mm\:ss");        // show how long before the cutoff the asset was changed, ignore if less than a minute as the 1 minute precision error in the filename datetime will show inconsistency  
                    }
                    newRow.Fields.Add(EscapeCharacters(timeSinceCutoff));                                       // add the time since cutoff, if the record was changed after the cuttoff this column is left blank

                    // now append the database fields ---------------------------------------                   // DB COLUMN HEADERS - append the database column names at the end, prefix each with an underscore to make these distinct from the columns which CORE consumes
                    foreach (string field in sourceRow.Fields) {
                        newRow.Fields.Add(EscapeCharacters(field));
                    }

                    // add the row to the recordset
                    targetData.Rows.Add(newRow);
                }

                //set the name of the tab
                targetData.Name = TAB1Name;

                // WRITE FILE - write to disk in base class
                targetData = base.WriteFile(targetData, overwrite: false);                                      // write without overwrite. Returns the lastchanged from the targetData recordset

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return targetData;
        }

        #endregion
    }
}
