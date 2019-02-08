using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LSAssetDataService {
    class LsawdDataReader : DataReader {
        #region VARIABLES & CONSTANTS -------------------------------------------------------------
        
        #endregion

        #region INITIALISATION --------------------------------------------------------------------

        public LsawdDataReader(string databaseServer, string assetDatabase, string lsDatabase, EventLogger logger) : base(databaseServer, assetDatabase, lsDatabase, logger) {
            // nothing to do here - all done in base
        }

        #endregion

        #region OVERRIDEN - methods overridden from superclass ------------------------------------

        // the specialised query string for each service, to retrieve assets for the period after the cutof date until now
        internal sealed override AssetDataRecordset ReadData(DateTime cutoff) { 

            const int retroActiveHours = 24;
            AssetDataRecordset assetRecords = null;

            // column positions in query
            const int SQLCode = 0, SQLStatusCode = 1, SQLLastChangeDate = 2, SQLCoreStatus = 4, SQLCoreModifiedDate = 6;

            try {
                /* LSAWD QUERY - The batch of records in the data file retrieved in each period, will contain:
	                 1) assets which have been updated in Agility after the previous cutoff time
	                 2) and, assets which have a 'null' workflow status in CORE (these assets would have just been provisioned in CORE)
	                 workflow status is retrieved for assets which exist in both CORE and Agility (the assets must have the same CORE UID and Agility Code) 
                     RETROACTIVE CUTOFF - the query will backdate and include a day of records before the cutoff time 
                     This is required to work around a functionality gap in the CORE import process: CORE imports only the most recent file even though there may be 
                     more than one new file produced since the last import, for example after an outage or if two service timer events occurred during the CORE 
                     import interval. The retroactive cutoff in the LSAWD query will therefore include all records changed in the 24 hours before the cutoff. 
                     to ensure that changes in a previous file which had not been imported, would be included by CORE when the latest file is imported, and CORE 
                     will update these assets retroactively.  Note however that the query will trigger a data file output only if assets had changed after the cutoff.
                 */
                DateTime retroActiveCutoff = cutoff.Subtract(TimeSpan.FromHours(retroActiveHours));         // set the retroactive cutoff to include previous changes 

                /* CONSTRUCT QUERY - gets results only if there were changes since the cutoff. If there were, also includes the changes since the retroativecutoff,
                 * in case CORE has not imported the previous extract (due to system being down or if the LSa*d service had stopped). */
                  
                string queryString = string.Format("IF EXISTS(SELECT TOP 1 AgilityAsset.Code, AgilityAsset.LastChangeDate FROM {0}..pmAsset AgilityAsset INNER JOIN "
                    + "(SELECT tbAsset.UID, tbAsset.WorkflowStatus FROM {1}..tbAsset WHERE tbAsset.IsDeleted = 0 AND tbAsset.UID IS NOT NULL) CoreAsset ON "
                    + "AgilityAsset.Code = CoreAsset.UID WHERE AgilityAsset.LastChangeDate > '{2}' OR CoreAsset.WorkflowStatus = 0) SELECT AgilityAsset.Code, "
                    + "AgilityStatus.StatusCode, AgilityAsset.LastChangeDate, CoreAsset.UID CoreUID, CoreAsset.StatusName CoreStatus, CoreAsset.RFIDTagID CoreRFID, "
                    + "CoreAsset.ModifiedDate CoreModifiedDate FROM {0}..pmAsset AgilityAsset INNER JOIN {0}..syAssetStatus AgilityStatus ON AgilityAsset.AssetStatusID = "
                    + "AgilityStatus.AssetStatusID INNER JOIN(SELECT tbAsset.UID, tbAsset.WorkflowStatus, tbWorkflowStatus.StatusName, tbRFIDBank.RFIDTagID, "
                    + "tbRFIDBank.IsDeleted RFIDIsDeleted, tbAsset.ModifiedDate FROM {1}..tbAsset LEFT JOIN {1}..tbRFIDBank ON tbAsset.RFIDBankIDF = tbRFIDBank.RFIDBankIDP "
                    + "LEFT JOIN {1}..tbWorkflowStatus ON tbAsset.WorkflowStatus = tbWorkflowStatus.WorkflowStatusIDP WHERE tbAsset.IsDeleted = 0 AND (tbRFIDBank.IsDeleted "
                    + "IS NULL OR tbRFIDBank.IsDeleted = 0)) CoreAsset ON AgilityAsset.Code = CoreAsset.UID WHERE(AgilityAsset.LastChangeDate > '{3}' OR "
                    + "CoreAsset.WorkflowStatus = 0) ORDER BY AgilityAsset.LastChangeDate DESC", AssetDatabase, LsDatabase, cutoff.ToString(SQLDateTimeFormat), retroActiveCutoff.ToString(SQLDateTimeFormat));
                // eg: cutoff date format : 2017-10-02 16:08:24.507

                /* EXECUTE QUERY in base class.. then add INDEX pointers for Asset attributes - these point each attribute to the relevant column in the assetrecord array 
                 * so that the file writer can access these without needing to understand each different query and its result data, which is 
                 * different in each service */
                assetRecords = base.ReadData(queryString);                                                  // execute overloaded method in base 

                if (assetRecords.Rows.Count > 0) {
                    assetRecords.Index.AssetCode = SQLCode;
                    assetRecords.Index.AssetOrParStatus = SQLStatusCode;
                    assetRecords.Index.LastChanged = SQLLastChangeDate;
                    assetRecords.Index.CoreWorkflowStatus = SQLCoreStatus;                                  // set the CORE workflow status as this needs to be inspected in the Filewriter to implement a workaround for defect 2894 (failing workflowstatus updates)
                    assetRecords.Index.CoreModifiedDate = SQLCoreModifiedDate;                              // use this to track assets which have just been provisioned in CORE and have a null workflow status, the CORE modified date is used to timestamp these record instead of the agility lastmodified 
                }

                assetRecords.LastModified = cutoff;                                                         // timestamp the result recordset based on the requested cutoff 
                                                                                                            Log.Trace(this.GetType().Name + Environment.NewLine + "\tqueryString=" + queryString + Environment.NewLine + "\tcutoff= " + cutoff.ToString(SQLDateTimeFormat) + Environment.NewLine + "\tretroActiveCutoff= " + retroActiveCutoff.ToString(SQLDateTimeFormat) + Environment.NewLine + "\tassetRecords.LastModified=" + assetRecords.LastModified.ToString(SQLDateTimeFormat) + Environment.NewLine, EventLogger.EventIdEnum.QUERYING_DATA);
            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return assetRecords;
        }
        #endregion

    }
}
