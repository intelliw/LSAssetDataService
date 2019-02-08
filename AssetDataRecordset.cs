
using System;
using System.Collections.Generic;
using System.Text;

namespace LSAssetDataService {
    /* provides a data transfer object for assets. AssetRecordset are created by a DataReader
     * query and used as input to a FileWriter to create an output file. The LSAssetData class 
     * implements a 'template' method design pattern (in the OnService event) and uses the AssetRecordset 
     * to transfer data between the query and output file as described above. The AssetRecordset entity includes 
     * accessors to retrieve raw query data and column headers, for the FileWriter to optionally include in 
     * the output file if needed.*/
    internal class AssetDataRecordset {

        #region VARIABLES & CONSTANTS ------------------------------------------------------------- 

        private List<string> columns = new List<string>();              // database column names corresponding to each element in the rows string array
        private List<AssetDataRow> rows = new List<AssetDataRow>();     // a list of assets for this recordset  
        private AssetAttributeIndex index = new AssetAttributeIndex();  // provides a pointer for each asset attribute to its column # 
        private DateTime latestModified;                                // set by the creator of this AssetDataRecordset
        private bool saved = false;                                     // set to true by the caller if the file is saved to disk
        private AssetDataRecordset supplementaryData;                   // supplementary datya for second tab
        private string name;                                            // the name of the recordset, if writing to xlsx this is used to set the name of the tab 

        #endregion

        #region INITIALISATION ----------------------------------------------------------------

        public AssetDataRecordset() {
        }
        


        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------

        internal List<AssetDataRow> Rows { get => rows; }
        internal List<string> Columns { get => columns; set => columns = value; }
        internal AssetAttributeIndex Index { get => index; set => index = value; }
        internal DateTime LastModified { get => latestModified; set => latestModified= value; }
        internal bool Saved { get => saved; set => saved = value; }
        internal AssetDataRecordset SupplementaryData { get => supplementaryData; set => supplementaryData = value; }
        internal string Name { get => name; set => name = value; }

        #endregion

        #region INNER CLASSES & ENUMS -------------------------------------------------------------

        /* the AssetAttributeIndex class stores pointers for each asset attribute to index the corresponding column 
         * in the asset data recordset */
        internal class AssetAttributeIndex {

            #region VARIABLES & CONSTANTS ------------------------------------------------------------- 

            const int DEFAULTColumnIndex = -1;                          // the default indicates that the column has not been indexed 

            // index varaiables - if indexed each of these will store a pointer to its column position in the columns array
            int assetCode = DEFAULTColumnIndex;              // Agility Code
            int assetOrParStatus = DEFAULTColumnIndex;       // Agility syAssetStatus.StatusCode | current PAR Status
            int lastChanged = DEFAULTColumnIndex;            // Agility LastChangeDate | last PAR Status change date
            int assetSublocationOrZone = DEFAULTColumnIndex; // Agility Sublocation | PAR Rule zone
            int department = DEFAULTColumnIndex;             // Agility HDWA_Department
            int rfidTagId = DEFAULTColumnIndex;              // Agility HDWA_RfidTag
            int serialNumber = DEFAULTColumnIndex;           // Agility SerialNumber
            int assetName = DEFAULTColumnIndex;              // Agility HDWA_AssetName
            int assetCategory = DEFAULTColumnIndex;          // Agility HDWA_Category | PAR Rule asset category
            int assetType = DEFAULTColumnIndex;              // Agility Type | PAR Rule asset type
            int assetModel = DEFAULTColumnIndex;             // Agility ModelNumber | PAR Rule asset model
            int coreUid = DEFAULTColumnIndex;                // CORE UID
            int coreRfid = DEFAULTColumnIndex;               // CORE RFID
            int coreModifiedDate = DEFAULTColumnIndex;       // CORE asset ModifiedDate  | PAR Rule modified date
            int coreCreatedDate = DEFAULTColumnIndex;        // CORE asset CreatedDate  
            int coreWorkflowStatus = DEFAULTColumnIndex;     // CORE asset StatusName
            int coreSublocationOrZone = DEFAULTColumnIndex;  // CORE Sublocation | Zone
            int coreLevel = DEFAULTColumnIndex;              // CORE Floor
            int parRule = DEFAULTColumnIndex;                // PAR Rule name   
            int parRuleStatus = DEFAULTColumnIndex;          // PAR Rule status 
            int parRuleQty = DEFAULTColumnIndex;             // PAR Rule quantity
            int parRuleRepQty = DEFAULTColumnIndex;          // PAR Rule replenishment quantity 
            int assetQuantity = DEFAULTColumnIndex;          // current asset quantity in PAR Zone
            int level = DEFAULTColumnIndex;                  // PAR Rule zone floor
            int zoneType = DEFAULTColumnIndex;               // PAR Rule zone type   
            int assetModelDescription = DEFAULTColumnIndex;  // PAR Rule asset model description
            int workflowStatus = DEFAULTColumnIndex;         // PAR Rule workflow statuses 
            int parRuleId = DEFAULTColumnIndex;              // PAR Rule id
            int assetModelOrTypeID = DEFAULTColumnIndex;     // PAR Rule asset model or type id
            int zoneId = DEFAULTColumnIndex;                 // PAR Rule zone id

            #endregion


            #region PUBLIC - external interfaces -------------------------------------------------------

            public int AssetCode { get => assetCode; set => assetCode = value; }
            public int AssetOrParStatus { get => assetOrParStatus; set => assetOrParStatus = value; }
            public int LastChanged { get => lastChanged; set => lastChanged = value; }
            public int AssetSublocationOrZone { get => assetSublocationOrZone; set => assetSublocationOrZone = value; }
            public int Department { get => department; set => department = value; }
            public int RfidTagId { get => rfidTagId; set => rfidTagId = value; }
            public int SerialNumber { get => serialNumber; set => serialNumber = value; }
            public int AssetName { get => assetName; set => assetName = value; }
            public int AssetCategory { get => assetCategory; set => assetCategory = value; }
            public int AssetType { get => assetType; set => assetType = value; }
            public int AssetModel { get => assetModel; set => assetModel = value; }
            public int CoreUID { get => coreUid; set => coreUid = value; }
            public int CoreRFID { get => coreRfid; set => coreRfid  = value; }
            public int CoreModifiedDate { get => coreModifiedDate; set => coreModifiedDate = value; }
            public int CoreCreatedDate { get => coreCreatedDate; set => coreCreatedDate = value; }
            public int CoreSublocationOrZone { get => coreSublocationOrZone; set => coreSublocationOrZone = value; }
            public int CoreLevel { get => coreLevel; set => coreLevel = value; }
            public int CoreWorkflowStatus { get => coreWorkflowStatus; set => coreWorkflowStatus = value; }
            public int ParRule { get => parRule; set => parRule = value; }               
            public int ParRuleStatus { get => parRuleStatus; set => parRuleStatus = value; }
            public int ParRuleQty { get => parRuleQty; set => parRuleQty = value; }
            public int ParRuleRepQty { get => parRuleRepQty; set => parRuleRepQty = value; }
            public int AssetQuantity { get => assetQuantity; set => assetQuantity = value; }
            public int Level { get => level; set => level = value; }
            public int ZoneType { get => zoneType; set => zoneType = value; }
            public int AssetModelDescription { get => assetModelDescription; set => assetModelDescription = value; }
            public int WorkflowStatus { get => workflowStatus; set => workflowStatus = value; }
            public int ParRuleId { get => parRuleId; set => parRuleId = value; }
            public int AssetModelOrTypeID { get => assetModelOrTypeID; set => assetModelOrTypeID = value; }
            public int ZoneId { get => zoneId; set => zoneId = value; }
            

            #endregion

        }
        #endregion
    }
}
