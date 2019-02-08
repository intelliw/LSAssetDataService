using System;
using System.ServiceProcess;

namespace LSAssetDataService {

    /* Executes data extraction at a specified interval after monitoring the server to verify that CORE is Active.
     * The service monitors CORE server status every 5 seconds. It executes asset data extracts when the data extraction interval elapses, 
     * only if the server is currently ACTIVE. All data extraction is supreessed if the CORE server is in STANDBY. 
     * The LS Asset Data Service must be running on both CORE servers so that the STANDBY instance can take over data extraction when CORE fails over to it.
     * The data extraction executes at the interval specified in service command line parameters. The default data extract interval is 20 seconds.
     * The OnBeforeInstall in ProjectInstaller.cs for documentation of command line parameters 
     */
    public class ServerMonitor {
        #region VARIABLES & CONSTANTS --------------------------------------------------------

        private EventLogger log;
        private ServerStatusEnum serverStatus = ServerStatusEnum.UNKNOWN;       // is set to ACTIVE if server is active, otherwise STANDBY. 

        private string masterServiceName;
        private const double SERVERMonitoringInterval = 5000;                   // monitor server status every 5 seconds
        #endregion

        #region INITIALISATION ---------------------------------------------------------------

        // construct the Monitor class
        public ServerMonitor(string masterService, EventLogger logger) {

            log = logger;
            masterServiceName = masterService;                                  // the service to monitor

            // START MONITORING                                                 // set up a monitoring timer to trigger OnServer() 
            System.Timers.Timer serverTimer = new System.Timers.Timer();
            serverTimer.Interval = SERVERMonitoringInterval;                    // must be less than Int32.MaxValue
            serverTimer.Elapsed += new System.Timers.ElapsedEventHandler(OnServer);
            serverTimer.Start();
        }
        #endregion

        #region PUBLIC - external interfaces --------------------------------------------------

        /* this event is called by the monitoring timer. 
         * It calls UpdateServerStatus to check whether the server is active 
         * i.e. checks if the 'master service' (CORE) is Running 
         * parameters.MasterServiceName provides the name of the monitored CORE service which indicates whether server is ACTIVE/ PASSIVE
         */
        public void OnServer(object sender, System.Timers.ElapsedEventArgs args) {

            UpdateServerStatus();

        }

        public ServerStatusEnum ServerStatus {
            get => serverStatus;
        }
        
        public enum ServerStatusEnum {
            UNKNOWN = 0,
            ACTIVE = 1,
            STANDBY = 2,
        }
        #endregion

        #region PRIVATE - internal functions --------------------------------------------------

        // updates serverStatus by checking whether server is active, by checking the master service 
        private void UpdateServerStatus() {

            // check master service 
            bool isActive = false;
            try {
                isActive = new ServiceController(masterServiceName).Status.Equals(ServiceControllerStatus.Running);

            } catch (Exception ex) {
                if (serverStatus != ServerStatusEnum.STANDBY) {                 // log exception first time or only when server was previously active
                    log.Error(ex.Message, EventLogger.EventIdEnum.MONITORING, ex);
                }
            }

            // update serverStatus and log status only if it changes 
            if (isActive) {
                if (serverStatus != ServerStatusEnum.ACTIVE) {                  // log status first time or only when there is a change  
                    serverStatus = ServerStatusEnum.ACTIVE;
                                                                                log.Info("\t" + Environment.MachineName + " is Active", EventLogger.EventIdEnum.MONITORING);
                }

            } else {
                if (serverStatus != ServerStatusEnum.STANDBY) {                 // log status first time or only when there is a change  
                    serverStatus = ServerStatusEnum.STANDBY;
                                                                                log.Warn("\t" + Environment.MachineName + " on Standby...", EventLogger.EventIdEnum.MONITORING);
                }
            }

        }
        
        #endregion

    }
}
