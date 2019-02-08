using System;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using System.Data;
using System.Collections.Generic;

namespace LSAssetDataService {

    /* monitors data in the asset management system and queries asset data changes 
     * since the last query was executed by the service. This class includes a function 
     * to check whether any data has changed since the data and time of the previous 
     * query ('last modified') */
    public class DataReader {

        #region VARIABLES & CONSTANTS -------------------------------------------------------------
        
        private EventLogger log;
        private string dbServer;
        private string assetDb;
        private string lsDb;

        private const string sqlDateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        #endregion

        #region INITIALISATION --------------------------------------------------------------------

        // CONSTRUCT data reader 
        public DataReader(string databaseServer, string assetDatabase, string lsDatabase, EventLogger logger) {
            dbServer = databaseServer;
            assetDb = assetDatabase;
            lsDb = lsDatabase;
            log = logger;
        }
        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------
        
        #endregion

        #region FINAL - cannot be overriden by subclass --------------------------------------------

        protected EventLogger Log { get => log; }                                   // sealed by not making the properties virtual

        internal string DatabaseServer { get => dbServer; }                         // asset and ls databases must both be on the same server
        internal string AssetDatabase { get => assetDb; }
        internal string LsDatabase { get => lsDb; }

        #endregion

        #region PRIVATE - internal functions ------------------------------------------------------

        // connection string is environment-specific and common to all services
        private string ConnectString() {

            // return string.Format("Data Source={0};Initial Catalog={1};Persist Security Info=True;User ID={2};Password='{3}'", server, database, user, pwd);
            return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=True", dbServer, assetDb);         // Data Source=wsc901usql;Initial Catalog=PCH_Agility_UAT;Integrated Security=True
        }
        #endregion

        #region VIRTUAL - must be overriden by subclass -------------------------------------------

        // method is overridden by subclass - the overloaded method implemented in this class is called by the subclass with a querystring
        internal virtual AssetDataRecordset ReadData(DateTime cutoff) {
            return null;
        }

        /* this overloaded method is called by the data reader subclass which first constructs a service-specific query string, 
         * then returns execution to this base method. The base method here will query and construct an asset data recordset 
         * in the same way for all services. It returns a set of query results for the period after the cutoff date specified in the 
         * superclasses overloaded ReadData method. The superclass method is called by the service event (e.g LSAsetWorkflowData.OnService) */
        internal AssetDataRecordset ReadData(string queryString = "") {
            
            AssetDataRecordset queryResults = new AssetDataRecordset();

            // query asset database. The returned asset recordset will have zero rows if there are no changed records  
            try {

                // CONNECT 
                string connectString = ConnectString();
                using (SqlConnection connection =
                    new SqlConnection(connectString)) {
                    connection.Open();


                    // QUERY without any locks 
                    using (SqlTransaction readTransaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted)) {  // use minimum isolation level 

                        using (SqlCommand command = new SqlCommand(queryString, connection)) {
                            command.Transaction = readTransaction;

                            using (SqlDataReader reader = command.ExecuteReader()) {
                                using (DataTable dataTable = new DataTable()) {
                                    dataTable.Load(reader);

                                    // add COLUMN HEADERS to the recordset
                                    int numColumns = dataTable.Columns.Count;
                                    for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
                                        queryResults.Columns.Add(dataTable.Columns[columnIndex].ColumnName);                // add database column names  
                                    }

                                    // add ASSET ROWS to the recordset
                                    for (int rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex++) {
                                        AssetDataRow assetRow = new AssetDataRow();                                         // create a new Asset, 
                                        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {

                                            if (dataTable.Rows[rowIndex][columnIndex] is DateTime) {
                                                assetRow.Fields.Add(((DateTime)dataTable.Rows[rowIndex][columnIndex]).ToString(SQLDateTimeFormat));  // add a precisely formatted date 
                                            } else {
                                                assetRow.Fields.Add(dataTable.Rows[rowIndex][columnIndex].ToString());      // add raw data for each field 
                                            }

                                        }
                                        queryResults.Rows.Add(assetRow);                                                    // add the row to the recordset
                                    }

                                }
                            }

                        }
                    }

                }

            } catch (Exception ex) {
                log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return queryResults;

        }
        #endregion

        #region INNER CLASSES & ENUMS -------------------------------------------------------------

        #endregion

        #region STATIC ----------------------------------------------------------------------------
        
        public static string SQLDateTimeFormat => sqlDateTimeFormat;
        #endregion

    }
}
