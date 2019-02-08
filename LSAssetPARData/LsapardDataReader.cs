using System;

namespace LSAssetDataService {
    class LsapardDataReader : DataReader {
        #region VARIABLES & CONSTANTS -------------------------------------------------------------
        
        #endregion

        #region INITIALISATION --------------------------------------------------------------------

        public LsapardDataReader(string databaseServer, string assetDatabase, string lsDatabase, EventLogger logger) : base(databaseServer, assetDatabase, lsDatabase, logger) {
            // nothing to do here - all done in base
        }

        #endregion

        #region OVERRIDEN - methods overridden from superclass ------------------------------------

        /* The data query produce a PAR Data Report, which provides a row with a count for every asset definition across all PAR Rules (i.e. every unique combination of Asset Category, Type, Model, 
         * & Workflow Status), in every zone, including zones in which the defined assets are present but do not have a PAR Rule defined. 
         * It produces a row for all asset and zone combinations in every PAR rule configured in CORE. 
         * The data query will execute if rfid movement was detected during the last refresh period (as defined by MAXMinuteBeforeRefresh constant below) 
         * or earlier if a PAR status changes since the last query was executed. 
         * If no PAR statuses have changed in 3 minutes the report will run anyway to refresh asset counts which may have changed due to workflow status changes in Agility.
         * If the report executes the results will include all zones and assets, not just the ones which had changed.
         * The results are sorted by Asset Category, Asset Type, Asset Model, Zone, & Current Status descending.
        */
        internal sealed override AssetDataRecordset ReadData(DateTime cutoff) { 

            AssetDataRecordset assetRecords = null;

            // refresh period
            const int MAXMinuteBeforeRefresh = 3;                       // if par statuses have not changed in 3 minutes run the report again anyway. this will refresh the counts, which may have changed due to workflowstatus changes

            // column positions in query
            const int SQLCurStatus = 0, SQLCurQty = 1, SQLParRuleStatus = 2, SQLParRuleQty = 3, SQLParRuleRepQty = 4,
                SQLLevel = 5, SQLZoneType = 6, SQLZone = 7,
                SQLAssetCategory = 8, SQLAssetType = 9, SQLAssetModel = 10, SQLAssetModelDesc = 11, SQLWorkflowStatuses = 12,
                SQLParRule = 13, SQLParRuleDate = 14, SQLCurStatusDate = 15;

            try {

                /* CONSTRUCT QUERY - gets results only if there were changes since the cutoff. */
                string queryString = string.Format("SELECT vwPARRuleCurStatus.CurStatus, vwAssetTagAssetMapCount.CurQty, vwPARRuleCurStatus.ParRuleStatus, vwPARRuleCurStatus.ParRuleQty, vwPARRuleCurStatus.ParRuleRepQty, "
                    + "COALESCE(vwAssetTagAssetMapCount.Level, vwPARRuleCurStatus.Level) Level, COALESCE(vwAssetTagAssetMapCount.ZoneType, vwPARRuleCurStatus.ZoneType) ZoneType, COALESCE(vwAssetTagAssetMapCount.Zone, vwPARRuleCurStatus.Zone) Zone, "
                    + "COALESCE(vwAssetTagAssetMapCount.AssetCategory, vwPARRuleCurStatus.AssetCategory) AssetCategory, COALESCE(vwAssetTagAssetMapCount.AssetType, vwPARRuleCurStatus.AssetType) AssetType, "
                    + "COALESCE(vwAssetTagAssetMapCount.AssetModel, vwPARRuleCurStatus.AssetModel) AssetModel, COALESCE(vwAssetTagAssetMapCount.AssetModelDescription, vwPARRuleCurStatus.AssetModelDescription) AssetModelDescription, "
                    + "COALESCE(vwAssetTagAssetMapCount.WorkflowStatuses, vwPARRuleCurStatus.WorkflowStatuses) WorkflowStatuses, vwPARRuleCurStatus.ParRule, vwPARRuleCurStatus.ParRuleDate, vwPARRuleCurStatus.CurStatusDate "
                    + "FROM(SELECT SUM(Count) CurQty, vwAssetTag.Level, vwAssetTag.ZoneType, vwAssetTag.Zone, vwAssetTag.AssetCategory, vwAssetTag.AssetType, vwAssetTag.AssetModel, vwAssetTag.AssetModelDescription, vwPARAssetMap.WorkflowStatuses, "
                    + "vwAssetTag.ZoneID, vwAssetTag.AssetCategoryID, vwAssetTag.AssetTypeID, vwAssetTag.AssetModelID FROM(SELECT Count(UID) Count, tbLocation.LocationName Level, tbEnum.Description ZoneType, tbSubLocation.SubLocationName Zone, "
                    + "tbAssetCategory.AssetCategoryName AssetCategory, tbAssetType.AssetTypeName AssetType, tbAssetModel.ModelName AssetModel, tbAssetModel.ModelDesc AssetModelDescription, tbWorkflowStatus.StatusName WorkflowStatus, "
                    + "tbAssetType.AssetCategoryIDF AssetCategoryID, tbAsset.AssetTypeIDF AssetTypeID, tbAsset.AssetModelIDF AssetModelID, tbAsset.WorkflowStatus WorkflowStatusID, tbRFIDBank.RFIDZoneIDF ZoneID FROM[{0}]..[tbAsset] "
                    + "INNER JOIN[{0}]..[tbRFIDBank] ON tbAsset.RFIDBankIDF = tbRFIDBank.RFIDBankIDP INNER JOIN[{0}]..[tbAssetModel] ON tbAsset.AssetModelIDF = tbAssetModel.AssetModelIDP AND tbAssetModel.IsDeleted IN (0, NULL) "
                    + "INNER JOIN[{0}]..[tbAssetType] ON tbAsset.AssetTypeIDF = tbAssetType.AssetTypeIDP AND tbAssetType.IsDeleted IN (0, NULL) INNER JOIN[{0}]..[tbAssetCategory] ON tbAssetType.AssetCategoryIDF = "
                    + "tbAssetCategory.AssetCategoryIDP AND tbAssetCategory.IsDeleted IN (0, NULL) INNER JOIN[{0}]..[tbSubLocation] ON tbRFIDBank.RFIDZoneIDF = tbSubLocation.SubLocationIDP AND tbSubLocation.IsDeleted = 0 "
                    + "INNER JOIN [{0}]..[tbEnum] ON tbSubLocation.SublocationType = Value AND tbEnum.EnumEntity = 'SubLocationType' INNER JOIN [{0}]..[tbLocation] ON tbLocation.LocationIDP = tbSubLocation.LocationIDF "
                    + "AND tbLocation.IsDeleted = 0 AND tbSubLocation.IsDeleted = 0 INNER JOIN [{0}]..[tbWorkflowStatus] ON tbAsset.WorkflowStatus = tbWorkflowStatus.WorkflowStatusIDP WHERE tbAsset.IsDeleted = 0 AND "
                    + "tbRFIDBank.IsDeleted = 0 AND tbAsset.RFIDBankIDF IS NOT NULL AND tbSubLocation.IsDeleted = 0 AND tbSubLocation.SublocationType IN (1,2,3) GROUP BY tbLocation.LocationName, tbEnum.Description, tbRFIDBank.RFIDZoneIDF, "
                    + "tbAssetType.AssetCategoryIDF, tbAsset.AssetTypeIDF, tbAsset.AssetModelIDF, tbAsset.WorkflowStatus, tbSubLocation.SubLocationName, tbAssetCategory.AssetCategoryName, tbAssetType.AssetTypeName, "
                    + "tbAssetModel.ModelName, tbAssetModel.ModelDesc, tbWorkflowStatus.StatusName) vwAssetTag INNER JOIN(SELECT DISTINCT tbAssetType.AssetCategoryIDF AssetCategoryID, tbAssetType.AssetTypeIDP AssetTypeID, "
                    + "tbAssetModel.AssetModelIDP AssetModelID, WfStatuses.WorkflowStatusID, WfStatuses.WorkflowStatuses FROM [{0}]..[tbPARAssetMap] INNER JOIN[{0}]..[tbAssetModel] "
                    + "ON tbPARAssetMap.PreferenceType = 1 AND tbPARAssetMap.ReferenceIDF = tbAssetModel.AssetModelIDP INNER JOIN [{0}]..[tbAssetType] ON((tbPARAssetMap.PreferenceType = 2 AND tbPARAssetMap.ReferenceIDF = tbAssetType.AssetTypeIDP) OR "
                    + "(tbPARAssetMap.PreferenceType = 1 AND tbAssetModel.AssetTypeIDF = tbAssetType.AssetTypeIDP)) AND tbAssetType.IsDeleted IN (0, NULL) INNER JOIN(SELECT tbPARCountByStatus.ParIDF, tbWorkflowStatus.StatusName WorkflowStatus, "
                    + "tbPARCountByStatus.WorkflowStatus WorkflowStatusID, tbWorkflowStatus.StatusName WokflowStatus, WorkflowStatuses = LTRIM(STUFF((SELECT ', ' + StatusName FROM[{0}]..[tbWorkflowStatus] "
                    + "INNER JOIN [{0}]..[tbPARCountByStatus] tbPARCountByStatus2 ON tbWorkflowStatus.WorkflowStatusIDP = tbPARCountByStatus2.WorkflowStatus WHERE tbPARCountByStatus.ParIDF = tbPARCountByStatus2.ParIDF AND "
                    + "tbWorkflowStatus.IsDeleted IN(0, NULL) ORDER BY StatusName FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')) FROM[{0}]..[tbPARCountByStatus] INNER JOIN[{0}]..[tbWorkflowStatus] "
                    + "ON tbWorkflowStatus.WorkflowStatusIDP = tbPARCountByStatus.WorkflowStatus GROUP BY tbPARCountByStatus.ParIDF, tbWorkflowStatus.StatusName, tbPARCountByStatus.WorkflowStatus) WfStatuses ON "
                    + "tbPARAssetMap.PARIDF = WfStatuses.ParIDF) vwPARAssetMap ON vwAssetTag.AssetCategoryID = vwPARAssetMap.AssetCategoryID AND vwAssetTag.AssetTypeID = vwPARAssetMap.AssetTypeID AND vwAssetTag.AssetModelID =  "
                    + "vwPARAssetMap.AssetModelID AND vwAssetTag.WorkflowStatusID = vwPARAssetMap.WorkflowStatusID GROUP BY vwPARAssetMap.WorkflowStatuses, vwAssetTag.Level, vwAssetTag.ZoneType, vwAssetTag.Zone, vwAssetTag.ZoneID, "
                    + "vwAssetTag.AssetCategory, vwAssetTag.AssetCategoryID, vwAssetTag.AssetType, vwAssetTag.AssetTypeID, vwAssetTag.AssetModel, vwAssetTag.AssetModelDescription, vwAssetTag.AssetModelID) vwAssetTagAssetMapCount "
                    + "FULL JOIN(SELECT DISTINCT vwZone.Level, vwZone.ZoneType, vwZone.Zone, vwPARCurStatus.AssetCategory, vwPARCurStatus.AssetType, vwPARCurStatus.AssetModel, vwPARCurStatus.AssetModelDescription, "
                    + "vwWorkflowStatuses.WorkflowStatuses, vwPARCurStatus.CurStatus, vwPARCurStatus.CurQty, vwPARCurStatus.CurStatusDate, vwPARRule.ParRule, vwPARRule.ParRuleStatus, vwPARRule.ParRuleQty, vwPARRule.ParRuleRepQty, "
                    + "vwPARRule.ParRuleDate, vwPARCurStatus.ZoneID, vwPARCurStatus.AssetCategoryID, vwPARCurStatus.AssetTypeID, vwPARCurStatus.AssetModelID FROM (SELECT tbPARCurrentStatus.PARIDF CurStatusPARID, "
                    + "tbPARCurrentStatus.SublocationIDF ZoneID, tbAssetCategory.AssetCategoryName AssetCategory, tbAssetType.AssetTypeName AssetType, tbAssetModel.ModelName AssetModel, tbAssetModel.ModelDesc AssetModelDescription, "
                    + "tbAssetType.AssetCategoryIDF AssetCategoryID, tbAssetType.AssetTypeIDP AssetTypeID, tbAssetModel.AssetModelIDP AssetModelID, tbEnum2.Description CurStatus, tbPARCurrentStatus.CurPARQty CurQty, "
                    + "tbPARCurrentStatus.StatusModifiedDate CurStatusDate FROM[{0}]..[tbPARCurrentStatus] LEFT JOIN[{0}]..[tbEnum] tbEnum2 ON tbPARCurrentStatus.PARStatus = tbEnum2.Value AND tbEnum2.EnumEntity = 'EnumPARStatus' "
                    + "LEFT JOIN[{0}]..[tbAssetModel] ON tbPARCurrentStatus.PreferenceType = 1 AND tbPARCurrentStatus.ReferenceIDF = tbAssetModel.AssetModelIDP INNER JOIN [{0}]..[tbAssetType] ON((tbPARCurrentStatus.PreferenceType = 2 "
                    + "AND tbPARCurrentStatus.ReferenceIDF = tbAssetType.AssetTypeIDP) OR(tbPARCurrentStatus.PreferenceType = 1 AND tbAssetModel.AssetTypeIDF = tbAssetType.AssetTypeIDP)) AND tbAssetType.IsDeleted IN (0, NULL) "
                    + "INNER JOIN[{0}]..[tbAssetCategory] ON tbAssetType.AssetCategoryIDF = tbAssetCategory.AssetCategoryIDP AND tbAssetCategory.IsDeleted IN (0, NULL) WHERE tbPARCurrentStatus.IsDeleted = 0) vwPARCurStatus "
                    + "LEFT JOIN(SELECT tbPAR.PARIDP RulePARID, tbPAR.PARName ParRule, tbEnum.Description ParRuleStatus, tbPAR.Quantity ParRuleQty, tbPAR.RepQuantity ParRuleRepQty, tbPAR.ModifiedDate ParRuleDate FROM[{0}]..[tbPAR] "
                    + "LEFT JOIN[{0}]..[tbEnum] ON tbPAR.PARStatus = tbEnum.Value AND tbEnum.EnumEntity = 'EnumPARStatus') vwPARRule ON(vwPARCurStatus.CurStatusPARID = vwPARRule.RulePARID) LEFT JOIN(SELECT tbSubLocation.SubLocationIDP "
                    + "ZoneID, tbLocation.LocationName Level, tbSubLocation.SubLocationName Zone, tbEnum.Description ZoneType FROM[{0}]..[tbSubLocation] INNER JOIN[{0}]..[tbEnum] "
                    + "ON tbSubLocation.SublocationType = Value AND tbEnum.EnumEntity = 'SubLocationType' INNER JOIN [{0}]..[tbLocation] ON tbLocation.LocationIDP = tbSubLocation.LocationIDF AND tbLocation.IsDeleted = 0 AND tbSubLocation.IsDeleted = 0) vwZone "
                    + "ON vwPARCurStatus.ZoneID = vwZone.ZoneID INNER JOIN(SELECT tbPARCountByStatus.ParIDF, tbWorkflowStatus.StatusName WorkflowStatus, tbPARCountByStatus.WorkflowStatus WorkflowStatusID, WorkflowStatuses = "
                    + "LTRIM(STUFF((SELECT ', ' + StatusName FROM[{0}]..[tbWorkflowStatus] INNER JOIN[{0}]..[tbPARCountByStatus] tbPARCountByStatus2 ON tbWorkflowStatus.WorkflowStatusIDP = tbPARCountByStatus2.WorkflowStatus "
                    + "WHERE tbPARCountByStatus.ParIDF = tbPARCountByStatus2.ParIDF AND tbWorkflowStatus.IsDeleted IN(0, NULL) ORDER BY StatusName FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')) FROM[{0}]..[tbPARCountByStatus] "
                    + "INNER JOIN[{0}]..[tbWorkflowStatus] ON tbWorkflowStatus.WorkflowStatusIDP = tbPARCountByStatus.WorkflowStatus GROUP BY tbPARCountByStatus.ParIDF, tbWorkflowStatus.StatusName, tbPARCountByStatus.WorkflowStatus "
                    + ") vwWorkflowStatuses ON vwPARCurStatus.CurStatusPARID = vwWorkflowStatuses.ParIDF) vwPARRuleCurStatus ON(vwAssetTagAssetMapCount.ZoneID = vwPARRuleCurStatus.ZoneID AND vwAssetTagAssetMapCount.AssetCategoryID = "
                    + "vwPARRuleCurStatus.AssetCategoryID AND vwAssetTagAssetMapCount.AssetTypeID = vwPARRuleCurStatus.AssetTypeID AND vwAssetTagAssetMapCount.AssetModelID = vwPARRuleCurStatus.AssetModelID AND "
                    + "vwAssetTagAssetMapCount.WorkflowStatuses = vwPARRuleCurStatus.WorkflowStatuses) ORDER BY COALESCE(vwAssetTagAssetMapCount.Level, vwPARRuleCurStatus.Level), COALESCE(vwAssetTagAssetMapCount.ZoneType, "
                    + "vwPARRuleCurStatus.ZoneType), COALESCE(vwAssetTagAssetMapCount.Zone, vwPARRuleCurStatus.Zone), COALESCE(vwAssetTagAssetMapCount.AssetCategory, vwPARRuleCurStatus.AssetCategory), "
                    + "COALESCE(vwAssetTagAssetMapCount.AssetType, vwPARRuleCurStatus.AssetType), COALESCE(vwAssetTagAssetMapCount.AssetModel, vwPARRuleCurStatus.AssetModel), "
                    + "COALESCE(vwAssetTagAssetMapCount.WorkflowStatuses, vwPARRuleCurStatus.WorkflowStatuses)", LsDatabase, cutoff.ToString(SQLDateTimeFormat), MAXMinuteBeforeRefresh.ToString());
                // eg: cutoff date format : 2017-10-02 16:08:24.507

                /* EXECUTE QUERY in base class.. then add INDEX pointers for Asset data attributes - these point each attribute to the relevant column in the assetrecord array 
                 * so that the file writer can access these without needing to understand each different query and its result data, which is 
                 * different in each service */
                assetRecords = base.ReadData(queryString);                                                  // execute overloaded method in base 

                if (assetRecords.Rows.Count > 0) {
                    assetRecords.Index.AssetOrParStatus = SQLCurStatus;
                    assetRecords.Index.AssetQuantity = SQLCurQty;

                    assetRecords.Index.ParRuleStatus = SQLParRuleStatus;
                    assetRecords.Index.ParRuleQty = SQLParRuleQty;
                    assetRecords.Index.ParRuleRepQty = SQLParRuleRepQty;

                    assetRecords.Index.Level = SQLLevel;
                    assetRecords.Index.AssetSublocationOrZone = SQLZone;
                    assetRecords.Index.ZoneType = SQLZoneType;

                    assetRecords.Index.AssetCategory = SQLAssetCategory;
                    assetRecords.Index.AssetType = SQLAssetType;
                    assetRecords.Index.AssetModel = SQLAssetModel;
                    assetRecords.Index.AssetModelDescription = SQLAssetModelDesc;
                    assetRecords.Index.WorkflowStatus = SQLWorkflowStatuses;

                    assetRecords.Index.ParRule = SQLParRule;
                    assetRecords.Index.CoreModifiedDate = SQLParRuleDate;
                    assetRecords.Index.LastChanged = SQLCurStatusDate;

                }

                assetRecords.LastModified = cutoff;                                                         // timestamp the result recordset based on the requested cutoff 
                                                                                                            Log.Trace(this.GetType().Name + Environment.NewLine + "\tqueryString=" + queryString + Environment.NewLine + "\tcutoff= " + cutoff.ToString(SQLDateTimeFormat) + Environment.NewLine + "\tassetRecords.LastModified=" + assetRecords.LastModified.ToString(SQLDateTimeFormat) + Environment.NewLine, EventLogger.EventIdEnum.QUERYING_DATA);
                // NOT USED - read supplementary data into a second recordset - NOT USED  
                // assetRecords.SupplementaryData = ReadSupplementaryData(cutoff);


            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return assetRecords;
        }

        /* NOTE USED- Reads optional data for the second tab in the data report - NOT USED  **/
        private AssetDataRecordset ReadSupplementaryData (DateTime cutoff) {

            AssetDataRecordset assetRecords = null;


            // column positions in query
            const int SQLCurQty = 0, SQLLevel = 1, SQLZoneType = 2, SQLZone = 3, SQLAssetCategory = 4, SQLAssetType = 5, SQLAssetModel = 6, SQLWorkflowStatuses = 7;

            try {

            /* CONSTRUCT QUERY */
            string queryString = string.Format("", LsDatabase);
                

                /* EXECUTE QUERY in base class.. then add INDEX pointers for Asset data attributes */
                assetRecords = base.ReadData(queryString);                                                  // execute overloaded method in base 

            if (assetRecords.Rows.Count > 0) {
                // assetRecords.Index.AssetQuantity = SQLCurQty;
            }

                assetRecords.LastModified = cutoff;                                                         // timestamp the result recordset based on the requested cutoff 
                                                                                                            Log.Trace(this.GetType().Name + Environment.NewLine + "\tqueryString=" + queryString + Environment.NewLine + "\tcutoff= " + cutoff.ToString(SQLDateTimeFormat) + Environment.NewLine + "\tassetRecords.LastModified=" + assetRecords.LastModified.ToString(SQLDateTimeFormat) + Environment.NewLine, EventLogger.EventIdEnum.QUERYING_DATA);
            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return assetRecords;
        }

        #endregion

    }
}
