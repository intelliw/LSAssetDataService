using System;
using System.Timers;
using System.Diagnostics;

/* Queries data at a specified interval after first monitoring the server to verify that CORE is Active.
 */
namespace LSAssetDataService {

    internal partial class LSAssetProvisioningData : LSAssetData {

        #region VARIABLES & CONSTANTS ------------------------------------------------------------- 

        private const string EVENTLogSourceName = "Asset Provisioning Data";            // source must be same as in 'LSAssetDataService-EventLog.ps1'

        #endregion

        #region INITIALISATION ----------------------------------------------------------------

        public LSAssetProvisioningData(string[] args) : base(args) {

            // create the filewriter and data reader for this service
            AssetDataFileWriter = new LsapdFileWriter(Parameters.TargetFolderPath, Parameters.RFIDCodePrefix, Log);
            AssetDataReader = new LsapdDataReader(Parameters.DatabaseServer, Parameters.AssetDatabaseName, Parameters.LSDatabaseName, Log);
            
        }
        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------

        #endregion

        #region OVERRIDEN - methods overridden from superclass --------------------------------------

        // ON SERVICE - execute the service event, called by the service timer 
        protected sealed override void OnService(object sender, ElapsedEventArgs args) {

            // execute service-specific function
            if (Monitor.ServerStatus == ServerMonitor.ServerStatusEnum.ACTIVE) {        // do nothing if server is on STANDBY 
                base.OnService();                                                       // call the overloaded superclass method
            }
        }

        // initialise service components and return an event logger reference
        protected sealed override void InitialiseComponents() {
            InitializeComponent();                                                      // initialise designer components in partial class 
            eventLog1 = new EventLog();                                                 // create EventLog componentfor this service 
        }

        // return the event log source name for this service 
        protected sealed override string EventLogSource => EVENTLogSourceName;

        // return the event log component for this service 
        protected sealed override EventLog EventLogComponent => eventLog1;

        #endregion
    } 
}
