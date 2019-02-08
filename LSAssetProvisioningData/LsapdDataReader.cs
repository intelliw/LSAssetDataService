using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LSAssetDataService {
    internal class LsapdDataReader : DataReader {
        #region VARIABLES & CONSTANTS -------------------------------------------------------------

        #endregion

        #region INITIALISATION ---------------------------------------------------------------------------------------------------

        public LsapdDataReader(string databaseServer, string assetDatabase, string lsDatabase, EventLogger logger) : base(databaseServer, assetDatabase, lsDatabase, logger) {
            // nothing to do here - all done in base
        }
        #endregion

        #region OVERRIDEN - methods overridden from superclass --------------------------------------

        /* retrieves assets for the period after the cutoff date until now.
        * TRACKABLE ASSETS - To be trackable an asset must be flagged for RFID tagging. To start CORE provisioning the asset code must be entered into 
        * the RFID tag field in agility. 
        *   To retag an asset (i.e to print a new tag) delete its RFID tag in CORE and 'change' the record in agility (i.e. update last changed date by opening 
        *       the asset and selecting 'change' and 'ok'). This will cause the asset to appear in the next asset provisionng data extract from Agility. 
        *       The adminsitrator can then reprint the tag and import the record into CORE (with the 'modify' option selected).
        *   To deprovision an asset the RFID tag field must be cleared and the corresponding asset and tag must be manually deleted in CORE
        *   Only assets with a category (HDWA_Category) of 'Facilities Management' or 'MTMU' are imported into CORE. If other categories are required the 
        *       SQL query below must be modified to include the categeory and its agility field mappings for asset category, type, and model; as these are 
        *       different for each asset category in agility data loads, and therefore require custom handling.*/
        internal sealed override AssetDataRecordset ReadData(DateTime cutoff) {

            AssetDataRecordset assetRecords = null;
            
            // column positions in query - if the query is modified these need to be updated as well
            const int SQLCode = 0, SQLLastChangeDate = 1, SQLCategory = 2, SQLType = 3;
            const int SQLModel = 4, SQLAssetName = 5, SQLRfidTag = 6, SQLDepartment = 7, SQLSerialNumber = 8, SQLSublocation = 9;
            const int SQLCoreUID = 10, SQLCoreRFID = 11, SQLCoreModifiedDate = 12, SQLCoreCreatedDate = 13, SQLCoreLevel = 14, SQLCoreZone = 15;
            
            // execute query 
            try {

                /* 
                 LSAPD QUERY - the extracted data file will include agility asset data for all of the following 
                   1. assets flagged for tracking in agility after the cutoff time (these do not exist in CORE)
                   2. assets which exist in CORE without an RFID tag (these were unlinked for retagging or reprovisioning)
                   3. assets which which were provisioned (created) in CORE after the cutoff time.

                   the query will be sorted by agility last modified or core created date 
                   when the data file is created later it will show 'New' in the __COREImportMethod column for assets which do not exist in core, and the LocationName and ZoneNamecolumn will 
                   display default zone for new assets, which is 'LB' and 'LBS02 [ICT Storeroom]' respectively.

                   the __COREImportMethod column will be 
                       'Modify (Retag)' if the asset exists in CORE and there is no linked RFID tag; 
                       'Modify (Reprovision)' if the asset exists in CORE and the asset and tag are linked. 
                       'New' if the asset has been flagged in Agility and does not exist in CORE

                       if the __COREImportMethod is 'Modify (Reprovision)' the LocationName and ZoneNamecolumn will cotnain the current Zone of the asset in CORE. This is to ensure that 
                       the CORE asset location is preserved correctly when the data file is imported into CORE (when the rest of the asset data requires update due to Agility changes, 
                       but not location)

                       if the __COREImportMethod is 'Modify (Retag)' or 'New' the LocationName and ZoneNamecolumn will be set to a default zone for new tags (an ICT Room in LB).
                       The data file should not be imported into CORE if the __COREImportMethod is 'Modify (Retag)' as this will overwrite the current zone location of the asset
                       with a default location. The retag option is typically needed only for reprinting new tags.

                       if the __COREImportMethod is 'New' or 'Modify (Reprovision)' importing the file will refresh CORE asset data and create a new tag and/or link the tag and asset.

                   As 'Modify (Reprovision)' import will overwrite the zone and location from the import file, it is important that the import is executed soon (if not immmediately) 
                   after the data is extracted. It is recommended that a new data file is generated for provisioning just before the administrator is ready to execute the import.
                   The instructions for this process are described in the TWI for asset provisioning.
                */
                string queryString = string.Format("IF EXISTS(SELECT AgilityAsset.Code, COREAsset.ModifiedDate, COREAsset.RFIDBankIDF FROM {0}..pmAsset AgilityAsset LEFT JOIN(SELECT tbAsset.UID, "
                    + "tbAsset.ModifiedDate, tbAsset.CreatedDate, tbAsset.RFIDBankIDF FROM {1}..tbAsset WHERE tbAsset.IsDeleted = 0) COREAsset ON AgilityAsset.Code = COREAsset.UID "
                    + "WHERE (COREAsset.UID IS NULL AND AgilityAsset.Code = AgilityAsset.HDWA_RfidTag AND AgilityAsset.LastChangeDate > '{2}') "
                    + "OR (COREAsset.UID IS NOT NULL AND COREAsset.RFIDBankIDF IS NULL AND COREAsset.ModifiedDate > '{2}') OR (COREAsset.UID IS NOT NULL AND COREAsset.CreatedDate > '{2}') "
                    + ") SELECT AgilityAsset.Code, AgilityAsset.LastChangeDate, AssetHierarchy.AssetCategory, AssetHierarchy.AssetType, AssetHierarchy.AssetModel, "
                    + "COALESCE(AgilityAsset.HDWA_AssetName, COREAssetName) AssetName, AgilityAsset.HDWA_RfidTag, AgilityAsset.HDWA_PrimaryUser, AgilityAsset.SerialNumber, pmSublocation.SublocationCode, "
                    + "COREAsset.UID COREUID, COREAsset.RFIDTagID CORERFID, COREAsset.ModifiedDate COREModifiedDate, COREAsset.CreatedDate CORECreatedDate, "
                    + "COREAsset.CORELevel, COREAsset.COREZone, COREAssetName FROM {0}..pmAsset AgilityAsset LEFT JOIN(SELECT pmAsset.Code, pmAsset.HDWA_Category AssetCategory, "
                    + "MTMUAssetType.ItemDesc AssetType, pmAsset.ModelNumber AssetModel FROM {0}..pmAsset INNER JOIN(SELECT DictItem.ItemCode, DictItem.ItemDesc "
                    + "FROM {0}..Dictionary INNER JOIN {0}..DictItem ON Dictionary.DictionaryID = DictItem.DictionaryID WHERE Dictionary.DictCode = 'HDWA_ModelType' "
                    + ") MTMUAssetType ON pmAsset.HDWA_ModelType = MTMUAssetType.ItemCode WHERE pmAsset.HDWA_Category = 'MTMU' UNION SELECT pmAsset.Code, pmAsset.HDWA_Category AssetCategory, "
                    + "pmAsset.HDWA_Keyword AssetType, pmAsset.Type AssetModel FROM {0}..pmAsset WHERE pmAsset.HDWA_Category = 'Facilities Management' AND pmAsset.HDWA_NamePlate <> 'LOC' "
                    + ") AssetHierarchy ON AgilityAsset.Code = AssetHierarchy.Code LEFT JOIN {0}..pmSublocation ON AgilityAsset.Sublocation = pmSublocation.SublocationID "
                    + "LEFT JOIN(SELECT tbAsset.UID, tbRFIDBank.RFIDTagID, tbAsset.RFIDBankIDF, tbAsset.ModifiedDate, tbAsset.CreatedDate, tbAsset.Field1 COREAssetName, "
                    + "tbLocation.LocationName CORELevel, tbSubLocation.SubLocationName COREZone FROM {1}..tbAsset LEFT JOIN {1}..tbRFIDBank ON RFIDBankIDF = RFIDBankIDP "
                    + "LEFT JOIN {1}..tbSubLocation ON tbRFIDBank.RFIDZoneIDF = tbSubLocation.SubLocationIDP AND tbSubLocation.IsDeleted = 0 LEFT JOIN {1}..tbLocation "
                    + "ON tbLocation.LocationIDP = tbSubLocation.LocationIDF AND tbLocation.IsDeleted = 0 AND tbSubLocation.IsDeleted = 0 WHERE tbAsset.IsDeleted = 0) COREAsset "
                    + "ON AgilityAsset.Code = COREAsset.UID WHERE(COREAsset.UID IS NULL AND AgilityAsset.Code = AgilityAsset.HDWA_RfidTag AND AgilityAsset.LastChangeDate > '{2}') "
                    + "OR (COREAsset.UID IS NOT NULL AND COREAsset.RFIDBankIDF IS NULL AND COREAsset.ModifiedDate > '{2}') OR (COREAsset.UID IS NOT NULL AND COREAsset.CreatedDate > '{2}') "
                    + "ORDER BY AgilityAsset.LastChangeDate DESC, COREAsset.ModifiedDate DESC", AssetDatabase, LsDatabase, cutoff.ToString(SQLDateTimeFormat));  // date format eg: 2017-10-02 16:08:24.507
                

                /* EXECUTE QUERY in base class.. then add INDEX pointers for Asset attributes - these point each attribute to the relevant column in the assetrecord array 
                 * so that the file writer can access these without needing to understand each different query and its result data, which is 
                 * different in each service */
                assetRecords = base.ReadData(queryString);                                      // execute overloaded method in base 
                
                if (assetRecords.Rows.Count > 0) {
                    assetRecords.Index.AssetCode = SQLCode;
                    assetRecords.Index.LastChanged = SQLLastChangeDate;                         // Agility timestamp
                    assetRecords.Index.AssetCategory = SQLCategory;                             // HDWA_Category    
                    assetRecords.Index.AssetType = SQLType;                                     // MTMU Type  - HDWA_ModelType
                    assetRecords.Index.AssetModel = SQLModel;                                   // MTMU Model - ModelNumber
                    assetRecords.Index.AssetName = SQLAssetName;
                    assetRecords.Index.RfidTagId = SQLRfidTag;
                    assetRecords.Index.Department = SQLDepartment;
                    assetRecords.Index.SerialNumber = SQLSerialNumber;
                    assetRecords.Index.AssetSublocationOrZone = SQLSublocation;
                    assetRecords.Index.CoreUID = SQLCoreUID;
                    assetRecords.Index.CoreRFID= SQLCoreRFID;
                    assetRecords.Index.CoreModifiedDate = SQLCoreModifiedDate;                  // CORE last modified timestamp
                    assetRecords.Index.CoreCreatedDate = SQLCoreCreatedDate;                    // CORE provisioning timestamp
                    assetRecords.Index.CoreSublocationOrZone = SQLCoreZone;                     // CORE zone for reprovisioning
                    assetRecords.Index.CoreLevel = SQLCoreLevel;                                // CORE level for reprovisioning
                }

                assetRecords.LastModified = cutoff;                                             // timestamp the result recordset based on the requested cutoff 
                                                                                                Log.Trace(this.GetType().Name + Environment.NewLine + "\tqueryString=" + queryString + Environment.NewLine + "\tcutoff= " + cutoff.ToString(SQLDateTimeFormat) + Environment.NewLine + "\tassetRecords.LastModified=" + assetRecords.LastModified.ToString(SQLDateTimeFormat) + Environment.NewLine, EventLogger.EventIdEnum.QUERYING_DATA);
            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return assetRecords;
        }
        #endregion

    }
}