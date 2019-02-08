using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LSAssetDataService {
    internal class LsapdFileWriter : FileWriter {

        #region VARIABLES & CONSTANTS -------------------------------------------------------------

        private const FileTypeEnum DATAFilenameExtension = FileTypeEnum.XLSX;           // e.g. AssetProvisioningDataFile_2017_08_10_10_01.xls

        private const string DATAFilenamePrefix = "AssetProvisioningDataFile";          // e.g. AssetProvisioningDataFile_2017_08_10_10_01.xlsx
        private const string ARCHIVESubFolder = "Archive";                              // folder must exist (use script 'LSAssetDataService-Registry and Folders.ps1')
        private const int FILESToRetainWhenArchiving = 8;                               // retain 8 files, to allow for approximately 2 day's worth of imports, assuming the service is conigured with a 6hr interval timer for 4 imports per day

        private const int RFIDBits = 16;                                                // total number of RFID bits required to be encoded for the RFIDTagID column
        private string rfidPrefix;                                                      // set in constructor based on paramters configured in the windows registry for this service 

        #endregion

        #region INITIALISATION ---------------------------------------------------------------

        // default constructor 
        public LsapdFileWriter(string outputFolder, string rfidLabelPrefix, EventLogger logger) : base(outputFolder, logger) {

            RfidPrefix = rfidLabelPrefix;                                               // The prefix is environment-specific, for example 'U' for UAT or 'P' for PROD, to ensure that printed tags are kept separate in each environment

            // nothing more to do here - all taken care of in base
        }


        #endregion

        #region PRIVATE - internal functions -------------------------------------------------------

        private string RfidPrefix { get => rfidPrefix; set => rfidPrefix = value; }
        #endregion


        #region OVERRIDEN - methods overridden from LSAssetData --------------------------------------

        // the prefix for the output filename
        internal sealed override string DataFilenamePrefix => DATAFilenamePrefix;

        // the prefix for the output filename
        internal sealed override FileTypeEnum DataFileType => DATAFilenameExtension;
        
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
         * The LSAPD output file structure is : 
         *  | LocationName | ZoneName | IsHuman | OwnershipName | RFIDTagID | SerialNumber | UID | AssetTypeName | AssetModelName | IsDynamic | AssetName
         * In addition the output file appends the database column names from the query result, to help with troubleshooting related to data issues
         * In addition the output file contains the following columns for the print template mappings
         *  __TagLocation | __AssetCategoryTagLabel | __ContactTagLabel | __IDTagLabel
         * If the asset name is missing in agility data it will be defaulted with the first 8 characters of the asset type, concatenated to the asset 
         * code (UID) e.g. 'Infusion MTM42946'
         * see comments in the LsapdDataReader.cs header for more documentaton on asset provisioning logic and options available
         */
        internal sealed override AssetDataRecordset WriteFile(AssetDataRecordset recordset, string targetFileSpec = "", bool overwrite = false) {

            /* note only these columns are consumed - the rest are required but not used in CORE due to a defect, 
             *      "LocationName", "ZoneName", "IsHuman", "OwnershipName", "RFIDTagID","SerialNumber", "UID", "AssetTypeName", "AssetModelName", "IsDynamic", "AssetName" */
            string[] COREFileHeaders = { "LocationName", "ZoneName", "IsHuman", "OwnershipName", "RFIDTagID",
                "SerialNumber", "UID", "AssetCategoryName", "AssetTypeName", "AssetModelName", "IsDynamic", "AssetName", "Barcode", "AssetComment",
                "Description", "PurchasePrice", "PurchaseDate", "ExpiryDate", "MarketValue", "CostPerDay", "RevenuePerDay", "IsSold", "IsLeased",
                "LeasedDate", "LeaseOwnershipName", "LeasedStartDate", "LeasedEndDate", "Comments", "Role", "FirstName", "LastName",
                "ContactNumber", "Gender", "Address1", "Address2", "City", "State", "Country", "PostCode", "Email", "BirthDate", "StaffID", "VisitorID" };

            const string tagSublocationColumnName = "__TagLocation";                                            // double underscore prefix for special columns      
            const string assetCategoryTagLabelColumnName = "__AssetCategoryTagLabel";                           // name of file column bound to MTMUCategoryLabel and FMCategoryLabel    
            const string contactTagLabelColumnName = "__ContactTagLabel";                                       // name of file column bound to MTMUContactLabel and FMContactLabel    
            const string IDTagLabelColumnName = "__IDTagLabel";                                                 // name of file column bound to MTMUIDLabel and FMIDLabel    
            const string COREImportMethodColumnName = "__COREImportMethod";                                     // how to import the record into CORE, whether New or Modify for Asset with RFID Tag

            const string DBColumnPrefix = "_";                                                                  // prefix database columns in the output file with an underscore to make these appear distinct from columns which CORE consumes
            const string FIELDSeparator = "-";                                                                  // separator between padded fields such as serial number 

            const string MTMUCategory = "MTMU";                                                                 // HDWA_Category value for MTMU 
            const string FMCategory = "Facilities Management";                                                  // HDWA_Category value for FM 

            const string MTMUCategoryTagLabel = "Medical Technology Management Unit";                           // value printed on the botom of MTMU tags
            const string FMCategoryTagLabel = "Facilities Management";                                          // value printed on the botom of FM tags

            const string MTMUIDTagLabel = "Tag No.";                                                            // label printed next to UID on MTMU tags
            const string FMIDTagLabel = "Asset Number";                                                         // label printed next to UID on MTMU tags

            const string MTMUContactTagLabel = "For equipment service call  6456 3242";                         // contact instruction printed on botom of MTMU tags
            const string FMContactTagLabel = "For equipment service support contact";                           // contact instruction printed on botom of FM tags

            const string LSMTMUCategory = "MTMU Equipment";                                                     // Category configured in CORE for MTMU 
            const string LSFMCategory = "FM Equipment";                                                         // Category configured in CORE for FM 
            const string UNKNOWN = "";                                                                          // unknwon category, still needs to be added to preserve column ordering 

            const string IMPORTMethodNew = "New";                                                               // the value to place in the __COREImportMethod column if the asset is New
            const string IMPORTMethodModifyReTag = "Modify (Retag)";                                            // value to be placeed in the __COREImportMethod column if asset exists without a RFID tag
            const string IMPORTMethodModifyReProvision = "Modify (Reprovision)";                                // value to be placeed in the __COREImportMethod column if asset exists WITH a RFID tag

            const int ASSETTYPE_CHARS_IN_ASSETNAME = 8;                                                         // the number of asset type characters to use in the asset name if the asset name was missign in agility data 

            // default values for required columns - if needed the adminstrator will bulk update these in the output xls before importing into CORE 
            const string DEFAULTMTMUDepartment = "MTMU";                                                        // Default to use if HDWA_Department is null, for an MTMU asset (HDWA_Category is 'Medical Technology Management Unit')
            const string DEFAULTFMDepartment = "Facilities Management";                                         // Default to use if HDWA_Department is null, for an FM asset (HDWA_Category is 'Facilities Management')
            const string DEFAULTZoneName = "LBS02 [ICT Storeroom]";                                             // the default storeroom zone to assign to new asset. This zone does not need to have any detection hardware.
            const string DEFAULTLocation = "LB";
            const string DEFAULTIsHuman = "No";
            const string DEFAULTIsDynamic = "Yes";
            const string RFIDTagIdDescriptionLabel = "RFID Tag ID: ";                                           // prefix added into the description field along with the rfid tag id

            const string TAB1Name = "Worksheet1";                                                               // the name of the first tab. Should not be changed as the Printer template has bindings which reference the worksheet by this name

            // create the output recordset                     
            AssetDataRecordset targetData = new AssetDataRecordset();
            targetData.LastModified = recordset.LastModified;                                                   // initialise with source recordset's cutoff time, which would be the timestamp used to filter the query, based on the file name produced by the previous extract

            // add the file headers and data rows into the targetData output recordset                  
            try {

                // COLUMNS -------------------------------------------------------------------                  // add specified columns to comply with the output file specifation, and database columns from the source recordset
                targetData.Columns = new List<string>(COREFileHeaders);                                         // CORE COLUMNS - first add the service-specific headers as specified by CORE 

                targetData.Columns.Add(EscapeCharacters(tagSublocationColumnName));                             // SUPPLEMENTARY COLUMNS - add "__TagLocation" 
                targetData.Columns.Add(EscapeCharacters(assetCategoryTagLabelColumnName));                      // "__AssetCategoryTagLabel"        
                targetData.Columns.Add(EscapeCharacters(contactTagLabelColumnName));                            // "__ContactTagLabel"
                targetData.Columns.Add(EscapeCharacters(IDTagLabelColumnName));                                 // "__IDTagLabel"
                targetData.Columns.Add(EscapeCharacters(COREImportMethodColumnName));                           // "__COREImportMethod"    

                foreach (string column in recordset.Columns) {                                                  // add DB COLUMN HEADERS - append the database column names, prefix each with an underscore to make these distinct from the columns which CORE consumes
                    targetData.Columns.Add(EscapeCharacters(DBColumnPrefix + column));                          
                }

                // ROWS ---------------------------------------------------------------------                   // add escaped data for each row
                foreach (AssetDataRow sourceRow in recordset.Rows) {

                    // catch exceptions for each row, and continue if there are errors 
                    try {
                        AssetDataRow newRow = new AssetDataRow();

                        // add the specified file fields ----------------------------------------                   // CORE COLUMNS - first add the service-specific headers as specified by CORE 
                        string category = sourceRow.Fields[recordset.Index.AssetCategory].Trim();
                        string department = sourceRow.Fields[recordset.Index.Department].Trim();
                        string uid = sourceRow.Fields[recordset.Index.AssetCode].Trim();
                        string serialNo = uid;                                                                      // serial number - CORE requires this to be unique so default it to the UID which is unique
                        string assetName = sourceRow.Fields[recordset.Index.AssetName].Trim();                      // asset name - agility data sometimes has no asset name and it needs to be defaulted later below 
                        string assetType = sourceRow.Fields[recordset.Index.AssetType].Trim();                      // asset type - used later to set default asset name if it is missing in agility data 
                        string assetModel = sourceRow.Fields[recordset.Index.AssetModel].Trim();                    // asset model - used later in validation and to set the asset name
                        string coreUid = sourceRow.Fields[recordset.Index.CoreUID].Trim();                          // empty if this is a new asset
                        string coreRfid = sourceRow.Fields[recordset.Index.CoreRFID].Trim();                        // empty if this is a new or unlinked asset, populated if the asset is being reprovisioned
                        string coreZone = sourceRow.Fields[recordset.Index.CoreSublocationOrZone].Trim();           // empty if this is a new or unlinked asset, populated if the asset is being reprovisioned
                        string coreLevel = sourceRow.Fields[recordset.Index.CoreLevel].Trim();                      // empty if this is a new or unlinked asset, populated if the asset is being reprovisioned

                        // validate asset hierarchy - there must be a category, type, and model 
                        if (String.IsNullOrEmpty(category) || String.IsNullOrEmpty(assetType) || String.IsNullOrEmpty(assetModel)) {
                            Log.Warn(uid + ": category, type, or model is missing." + Environment.NewLine + "Skipping and continuing..", EventLogger.EventIdEnum.QUERYING_DATA);
                            continue;                                                                               // skip this record
                        }
                        
                        // track whether this row is 'new' or retag/reprovision (i.e. 'modify..') 
                        bool reTag = !String.IsNullOrEmpty(coreUid) && String.IsNullOrEmpty(coreRfid);              // if asset exists in CORE and does not have a RFID tag (tag must have been unlinked) then it is being retagged
                        bool reProvision = !String.IsNullOrEmpty(coreUid) && !String.IsNullOrEmpty(coreRfid);       // if asset exists in CORE and has a RFID tag then it is being reprovisioned (by backdating the extract timestamp)

                        // set the importmethod, and timestamp for CORE's import workaround/ defect
                        string importMethod = IMPORTMethodNew;                                                      // default to New 
                        DateTime rowLastChanged = DateTime.Parse(sourceRow.Fields[recordset.Index.LastChanged]);    // default to agilty's date/time
                        if (reTag) {                                                                                // retag means asset exists without a RFID tag    
                            importMethod = IMPORTMethodModifyReTag;                                                 // set import method 
                            rowLastChanged = DateTime.Parse(sourceRow.Fields[recordset.Index.CoreModifiedDate]);    // track CORE modified timestamp 
                        } else if (reProvision) {                                                                   // reprovision means asset exists WITH a RFID tag        
                            importMethod = IMPORTMethodModifyReProvision;                                           // set import method 
                            rowLastChanged = DateTime.Parse(sourceRow.Fields[recordset.Index.CoreCreatedDate]);     // track CORE created timestamp 
                        }

                        // set zone and location. if reprovisioning the zone should be the current zone in CORE
                        string levelName = DEFAULTLocation;                                                         // default Location (LB), for new or unlinked assets only 
                        string zoneName = DEFAULTZoneName;                                                          // default Zone (LB ICT room), for new or unlinked assets only 
                        if (importMethod == IMPORTMethodModifyReProvision) {                                        // if the asset is being reprovisioned set the zone and level to preserve CORE's location on import 
                            levelName = coreLevel;
                            zoneName = coreZone;
                        }

                        // add data columns into this row
                        newRow.Fields.Add(EscapeCharacters(levelName));                                             // column 1 - "LocationName" - new assets always added to a default location, which does not have RFID detection
                        newRow.Fields.Add(EscapeCharacters(zoneName));                                              // column 2 - "ZoneName" - default zone, same as LocationName 
                        newRow.Fields.Add(EscapeCharacters(DEFAULTIsHuman));                                        // column 3 - "IsHuman" - 'No' for Assets

                        if (String.IsNullOrEmpty(department)) {                                                     // if department is null use defaults, as Ownership is mandatory in CORE 
                            if (category == MTMUCategory) {                                                         // if category is MTMU..
                                department = DEFAULTMTMUDepartment;                                                 // ..default department to MTMU
                            } else if (category == FMCategory) {                                                    // if category is FM..    
                                department = DEFAULTFMDepartment;                                                   // ..default department to FM
                            }
                        }
                        newRow.Fields.Add(EscapeCharacters(department));                                            // column 4 - "OwnershipName" - (OwnershipName is Department in Agility) (see above)


                        string rfidTagId = HexEncodeAsciiID(uid, RfidPrefix, RFIDBits);
                        newRow.Fields.Add(rfidTagId);                                                               // column 5 - "RFIDTagID" hex encode the prefix and the UID upto max number of bits

                        if (!sourceRow.Fields[recordset.Index.SerialNumber].TrimEnd().Equals("")) {                 // if agility serial number is not null 
                            serialNo += FIELDSeparator + sourceRow.Fields[recordset.Index.SerialNumber];            // concatenate uid and serial number with a separator
                        }
                        newRow.Fields.Add(EscapeCharacters(serialNo));                                              // column 6 - "SerialNumber".... (see above)

                        newRow.Fields.Add(EscapeCharacters(uid));                                                   // column 7 - "UID" 

                        // only FM and MTMU asset categories are supported (see lsapdDataReader documentation)..     
                        if (category == MTMUCategory) {                                                             // MTMU asset category.. 
                            newRow.Fields.Add(EscapeCharacters(LSMTMUCategory));                                    // column 8 - "AssetCategoryName" (MTMU) - set the LS category for MTMU
                        } else if (category == FMCategory) {                                                        // FM asset category.. 
                            newRow.Fields.Add(EscapeCharacters(LSFMCategory));                                      // column 8 - "AssetCategoryName" (FM) - set the LS category for FM
                        } else {
                            newRow.Fields.Add(EscapeCharacters(UNKNOWN));                                           // if unknow, still need to add field to preserve column ordering  
                        }

                        newRow.Fields.Add(EscapeCharacters(assetType));                                             // column 9 - "AssetTypeName" 
                        newRow.Fields.Add(EscapeCharacters(assetModel));                                            // column 10 - "AssetModelName" 

                        newRow.Fields.Add(EscapeCharacters(DEFAULTIsDynamic));                                      // column 11 - "IsDynamic" 

                        if (String.IsNullOrEmpty(assetName)) {                                                      // if assetname is empty set to uid  
                            assetName = assetType.Substring(0, ASSETTYPE_CHARS_IN_ASSETNAME) + " " + uid;           // default to first 8 characters of asset type concatenated with asset code e.g. 'Infusion MTM42946'
                        }
                        newRow.Fields.Add(EscapeCharacters(assetName));                                             // column 12 - "AssetName" 

                        // keep a tab on the latest change to use in the output file                
                        if (rowLastChanged > targetData.LastModified) { targetData.LastModified = rowLastChanged; }

                        // add empty fields Barcode and AssetComment
                        newRow.Fields.Add("");                                                                      // column 13 - "Barcode"    
                        newRow.Fields.Add("");                                                                      // column 14 - "AssetComment"    

                        // add RFID Tag ID into description field. This information is referenced to relink the asset with its tag, after unlinking, extracting data, and printing the tag 
                        newRow.Fields.Add(RFIDTagIdDescriptionLabel + rfidTagId);                                   // column 15 - "Description"    (e.g. 'RFID Tag ID: 5545443031323030'

                        // add remaining empty fields which are not used 
                        int numEmptyCols = COREFileHeaders.Length - newRow.Fields.Count;                            // the remaining columns are unused but CORE requires them to be present in the export file
                        newRow.Fields.AddRange(new List<String>(new string[numEmptyCols]));                         // so add empty fields for these columns

                        // now append the supplementary fields -------------------------------                      // SUPPLEMENTARY COLUMNS - double underscore prefix for special columns 

                        if (category == MTMUCategory) {
                            newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.Department]));      // "__TagLocation"              - MTMU tags print department (i.e.HDWA_PrimaryUser)
                            newRow.Fields.Add(EscapeCharacters(MTMUCategoryTagLabel));                              // "__AssetCategoryTagLabel"    - MTMU tags print "Medical Technology Management Unit"
                            newRow.Fields.Add(EscapeCharacters(MTMUContactTagLabel));                               // "__ContactTagLabel"
                            newRow.Fields.Add(EscapeCharacters(MTMUIDTagLabel));                                    // "__IDTagLabel"
                        } else if (category == FMCategory) {
                            newRow.Fields.Add(EscapeCharacters(sourceRow.Fields[recordset.Index.AssetSublocationOrZone]));// "__TagLocation"              - FM tags print sublocation 
                            newRow.Fields.Add(EscapeCharacters(FMCategoryTagLabel));                                // "__AssetCategoryTagLabel"    - FM tags print "Facilities Management"
                            newRow.Fields.Add(EscapeCharacters(FMContactTagLabel));                                 // "__ContactTagLabel"
                            newRow.Fields.Add(EscapeCharacters(FMIDTagLabel));                                      // "__IDTagLabel"
                        } else {
                            newRow.Fields.Add(EscapeCharacters(UNKNOWN));                                           // __TagLocation - if unknow, still need to add field to preserve column ordering  
                            newRow.Fields.Add(EscapeCharacters(UNKNOWN));                                           // __AssetCategoryTagLabel - if unknow, still need to add field to preserve column ordering  
                            newRow.Fields.Add(EscapeCharacters(UNKNOWN));                                           // __ContactTagLabel - if unknow, still need to add field to preserve column ordering  
                            newRow.Fields.Add(EscapeCharacters(UNKNOWN));                                           // __IDTagLabel - if unknow, still need to add field to preserve column ordering  
                        }
                        newRow.Fields.Add(EscapeCharacters(importMethod));                                          // __COREImportMethod - new or modify 

                        // now append the database fields ------------------------------------                      // DB COLUMN HEADERS - append the database column names at the end, prefix each with an underscore to make these distinct from the columns which CORE consumes
                        foreach (string field in sourceRow.Fields) {
                            newRow.Fields.Add(EscapeCharacters(field));
                        }

                        // add the row to the recordset
                        targetData.Rows.Add(newRow);


                    } catch (Exception ex) {
                        Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
                    }
                }

                //set the name of the tab
                targetData.Name = TAB1Name;

                // WRITE FILE - write to disk in base class
                targetData = base.WriteFile(targetData, overwrite:false);                                            // write without overwrite. Returns the lastchanged from the targetData recordset

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return targetData;
        }

        #endregion
    }
}
