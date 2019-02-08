using System;
using System.Diagnostics;
using System.ServiceProcess;
using System.Runtime.InteropServices;

namespace LSAssetDataService {

    internal class LSAssetData : ServiceBase {

        #region VARIABLES & CONSTANTS -------------------------------------------------------------

        private ServiceParameters parameters;
        private ServiceStatusUpdater status; 
        private EventLogger log;
        private ServerMonitor monitor;
        private FileWriter fw;                                          // file writer - set by each service in its constructor
        private DataReader dm;                                          // data monitor - set by each service in its constructor    
        private DateTime latestModifiedAssetTime;                       // assigned in OnStart(). This variable stores the full date and time of the latest asset changein Agility. It is needed to provide a more accurate cuttoff than the fileLatestModifiedTime from the CORE file timestamp, which is accurate to the minute only. The precise latest modified time is used in the ReadData query instead of the timestamp from the file. This prevents unnecessary repeat reads of the latest record when it has not changed.

        private string serviceDisplayName;

        #endregion

        #region INITIALISATION --------------------------------------------------------------------

        public LSAssetData() {
            // base(..) gets called first !
        }
        
        /* initialises the service's event log, service parameters, and service status updater
         * The event log and source cannot be created and immediately used here as latency time 
         * is needed to enable the log. Instead use the provided script 'LSAssetDataService-EventLog.ps1' 
         * to create the log and source prior to starting the service.
         * Parameters are set from command line arguments passed in through 'args'.    */
        public LSAssetData(string[] args) {

            // INITIALISE SERVICE
            InitialiseComponents();                                                     // initialises low level components from designer    
            serviceDisplayName = new ServiceController(ServiceName).DisplayName;        // display name created by project installer 

            // create aggregates: EVENT LOG, PARAMETERS, SERVICE STATUS UPDATER 
            log = new EventLogger(EventLogComponent, EventLogSource);
            parameters = new ServiceParameters(args, log);                                   
            status = new ServiceStatusUpdater(this);                                    
                                                                                        // LOG details 
                                                                                        log.Info("Service Parameters:"
                                                                                            + Environment.NewLine + "\tMonitored Service=" + parameters.MasterServiceName 
                                                                                            + Environment.NewLine + "\tInterval Minutes=" + parameters.IntervalMinutes 
                                                                                            + Environment.NewLine + "\tInterval Seconds=" + parameters.IntervalSeconds
                                                                                            + Environment.NewLine + "\tTotal interval (milliseconds)=" + parameters.IntervalMilliseconds
                                                                                            + Environment.NewLine + "\tTarget Folder Path=" + parameters.TargetFolderPath 
                                                                                            + Environment.NewLine + "\tDatabase Server=" + parameters.DatabaseServer
                                                                                            + Environment.NewLine + "\tAsset Database=" + parameters.AssetDatabaseName
                                                                                            + Environment.NewLine + "\tLS Database=" + parameters.LSDatabaseName
                                                                                            + Environment.NewLine + "\tRFID Tag Prefix=" + parameters.RFIDCodePrefix
                                                                                            + Environment.NewLine + "Event Logger:"
                                                                                            + Environment.NewLine + "\tEvent Log Name=" + log.EventLogName 
                                                                                            + Environment.NewLine + "\tEvent Log Source=" + log.EventSourceName 
                                                                                            + Environment.NewLine + "Service Status Updater:"
                                                                                            + Environment.NewLine + "\tCurrent Status=" + Enum.GetName(typeof(ServiceStatusUpdater.ServiceStateEnum), status.GetStatus)
                                                                                            , EventLogger.EventIdEnum.INITIALISING);
        }
        
        // ON START - starts the service and server monitor, and executes the service for the first time 
        protected override void OnStart(string[] args) {
            status.StartPending();
           
            // START SERVER MONITOR
            monitor = new ServerMonitor(parameters.MasterServiceName, Log);             // monitor the master service, OnStop disposes of this
            
            // START SERVICE timer                                                      // activates the service timer to trigger OnService()     
            System.Timers.Timer serviceTimer = new System.Timers.Timer();
            serviceTimer.Interval = parameters.IntervalMilliseconds;                    // must be less than Int32.MaxValue
            serviceTimer.Elapsed += new System.Timers.ElapsedEventHandler(OnService);
            serviceTimer.Start();
                                                                                         // LOG STARTUP details 
                                                                                         log.Info("\t" + ServiceName + " (" + serviceDisplayName + ") started on " + Environment.MachineName
                                                                                            + Environment.NewLine + "Server Monitor:"
                                                                                            + Environment.NewLine + "\tMonitoring " + Environment.MachineName + " failover status"
                                                                                            + Environment.NewLine + "File Writer:"
                                                                                            + Environment.NewLine + "\tOutput Folder=" + AssetDataFileWriter.OutputFolderPath
                                                                                            + Environment.NewLine + "\tArchive Folder=" + AssetDataFileWriter.ArchiveFolderPath
                                                                                            + Environment.NewLine + "\tFiles To Retain When Archiving=" + AssetDataFileWriter.FilesToRetainWhenArchiving 
                                                                                            + Environment.NewLine + "\tFilename Prefix=" + AssetDataFileWriter.DataFilenamePrefix
                                                                                            + Environment.NewLine + "\tFilename Extension=" + AssetDataFileWriter.DataFileType
                                                                                            + Environment.NewLine + "Data Reader:"
                                                                                            + Environment.NewLine + "\tDatabase Server=" + AssetDataReader.DatabaseServer
                                                                                            + Environment.NewLine + "\tAsset Database=" + AssetDataReader.AssetDatabase
                                                                                            + Environment.NewLine + "\tLS Database=" + AssetDataReader.LsDatabase
                                                                                            , EventLogger.EventIdEnum.STARTING);
            
            // initiliase CUTOFF time - this variable stores the full date and time of the latest asset changein Agility. It is needed to provide a more accurate cuttoff than the fileLatestModifiedTime from the CORE file timestamp, which is accurate to the minute only. The precise latest modified time is used in the ReadData query instead of the timestamp from the file. This prevents unnecessary repeat reads of the latest record when it has not changed.
            LatestModifiedAssetTime = AssetDataFileWriter.RetrieveLatestModifiedFileTime();
            

            status.Running();


            // execute the service - note the monitor may not have started in which case wait for service timer to execute next onservice 
            if (monitor.ServerStatus == ServerMonitor.ServerStatusEnum.ACTIVE) {          // do nothing if server is on STANDBY 
                OnService();                                                              // otherwise execute the service 
            }

        }
        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------

        #endregion

        #region FINAL - cannot be overriden by subclass --------------------------------------------

        protected EventLogger Log { get => log; }                                         // sealed by not making the properties virtual
        protected ServerMonitor Monitor { get => monitor; }
        protected ServiceStatusUpdater Status { get => status; }
        protected ServiceParameters Parameters { get => parameters; }
        
        protected FileWriter AssetDataFileWriter { get => fw; set => fw = value; }        // the file writer instantiated by each service class 
        protected DataReader AssetDataReader { get => dm; set => dm = value; }            // the file writer instantiated by each service class 

        #endregion
         

        #region VIRTUAL - must be overriden by subclass --------------------------------------------
        
        // method is overridden by subclass - the overloaded method implemented in this class is called by the subclass without any arguments
        protected virtual void OnService(object sender, System.Timers.ElapsedEventArgs args) {   // all services will override this method
        }

        // ON SERVICE - this is an overloaded final method. The subclass method is called by the service event, which is triggered by the service timer 
        protected void OnService() {                                                      // overloaded method called by the sublcass 
            const int ONEMinutePrecision = 1;                                             // used to compare the datetime in the output filename which is accurate to 1 minute, with an internal variable, which includes seconds & millseconds 
             
            try { 
                
                // ARCHIVE - always archive files before creating output 
                AssetDataFileWriter.ArchiveFiles();

                // GET CUTOFF - as CORE file timestamp has minutes and no seconds or milliseconds. Using the precise time of the latest modified asset avoids repeat reading of the same asset when it has not changed
                DateTime cutoff = AssetDataFileWriter.RetrieveLatestModifiedFileTime();   // by default use the file time (see FileWriter.RetrieveLastModifiedTime for description of logic used) 
                if (LatestModifiedAssetTime > cutoff) { 
                    if (LatestModifiedAssetTime.Subtract(cutoff).TotalMinutes < ONEMinutePrecision) {  // if the filetime is within 1 minute (less) than the last processed asset record from the previous service iteration), use the asset's precise lastmodified time instead as it is more precise
                        cutoff = LatestModifiedAssetTime;                                 // use the asset's precise lastmodified time instead 
                    }
                }
                                                                                          Log.Trace(this.GetType().Name + Environment.NewLine + "\tcutoff= " + cutoff.ToString(FileWriter.ISO8601DateTimeFormat) + Environment.NewLine + "\tlatestModifiedAssetTime= " + LatestModifiedAssetTime.ToString(FileWriter.ISO8601DateTimeFormat) + Environment.NewLine + "\tAssetDataFileWriter.RetrieveLatestModifiedFileTime= " + AssetDataFileWriter.RetrieveLatestModifiedFileTime().ToString(FileWriter.ISO8601DateTimeFormat) + Environment.NewLine + "\tMinutes= " + (LatestModifiedAssetTime - AssetDataFileWriter.RetrieveLatestModifiedFileTime()).Minutes, EventLogger.EventIdEnum.EXECUTING_SERVICE);

                // QUERY DATABASE - gets changed Asset records if there were any changes since the cutoff
                AssetDataRecordset changedAssets = AssetDataReader.ReadData(cutoff);      // excecute query with last modified date/time as cutoff
                                                                                          Log.Trace(this.GetType().Name + Environment.NewLine + "ReadData(cutofff) [changedAssets]: " + changedAssets.LastModified.ToString(DataReader.SQLDateTimeFormat) + Environment.NewLine + "\trows: " + changedAssets.Rows.Count + Environment.NewLine + "\tcolumns: " + changedAssets.Columns.Count, EventLogger.EventIdEnum.EXECUTING_SERVICE);
                // CREATE OUTPUT FILE - with rows for the changed assets retrieved above
                if (changedAssets.Rows.Count > 0) {                                       // if there were changed assets
                    AssetDataRecordset writtenAssets = AssetDataFileWriter.WriteFile(changedAssets);    // update the precise latestModifiedTime - which was also used in less precise format to stamp the file name
                    if (writtenAssets.Saved == true) {                                    // update LatestModifiedAssetTime only if the file was saved   
                        LatestModifiedAssetTime = writtenAssets.LastModified;             // update the instance variable for the timestamp 
                    }

                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.EXECUTING_SERVICE, ex);
            }

        } 

        // initialises components created by designer in partial classes for each service 
        protected virtual void InitialiseComponents() {
        }
        
        // overriden by each individual service - the name of the event source for each individual service 
        protected virtual string EventLogSource {
            get;
        }
        
        // the event log component instantiated by each service class 
        protected virtual EventLog EventLogComponent {
            get;
        }
        public DateTime LatestModifiedAssetTime { get => latestModifiedAssetTime; set => latestModifiedAssetTime = value; }

        #endregion


        #region OVERRIDEN - methods overridden from superclass --------------------------------------

        protected sealed override void OnStop() {
            status.StopPending();
            monitor = null;                                                                 // dispose, will be recreated by OnStart
            status.Stopped();
                                                                                            Log.Info("\t" + ServiceName + " (" + serviceDisplayName + ") stopped on " + Environment.MachineName, EventLogger.EventIdEnum.STOPPING);
        }
        protected sealed override void OnContinue() {
            status.ContinuePending();
                                                                                            Log.Info("\t" + ServiceName + " (" + serviceDisplayName + ") continued on " + Environment.MachineName, EventLogger.EventIdEnum.CONTINUING);
        }
        protected sealed override void OnPause() {
            status.PausePending();
                                                                                            Log.Info("\t" + ServiceName + " (" + serviceDisplayName + ") paused on " + Environment.MachineName, EventLogger.EventIdEnum.PAUSING);
        }
                
        #endregion


        #region INNER CLASSES & ENUMS ----------------------------------------------------------------

        // update status in Service Control Manager window 
        protected class ServiceStatusUpdater {

            [DllImport("advapi32.dll", SetLastError = true)]
            private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);

            private LSAssetData svc;
            private ServiceStateEnum status = ServiceStateEnum.SERVICE_START_PENDING;
            
            public ServiceStatusUpdater(LSAssetData svc) {
                this.svc = svc;
            }
            public void Stopped() {
                status = ServiceStateEnum.SERVICE_STOPPED;
                SetStatus(status);
            }
            public void Running() {
                status = ServiceStateEnum.SERVICE_RUNNING;
                SetStatus(status);
            }
            public void Paused() {
                status = ServiceStateEnum.SERVICE_PAUSED;
                SetStatus(status);
            }
            public void StartPending() {
                status = ServiceStateEnum.SERVICE_START_PENDING;
                SetStatus(status);
            }
            public void StopPending() {
                status = ServiceStateEnum.SERVICE_STOP_PENDING;
                SetStatus(status);
            }
            public void ContinuePending() {
                status = ServiceStateEnum.SERVICE_CONTINUE_PENDING;
                SetStatus(status);
            }
            public void PausePending() {
                status = ServiceStateEnum.SERVICE_PAUSE_PENDING;
                SetStatus(status);
            }

            // report status to Service Control Manager window
            protected void SetStatus(ServiceStateEnum newStatus) {
                ServiceStatus serviceStatus = new ServiceStatus() {
                    dwCurrentState = newStatus,
                    dwWaitHint = 100000,
                };
                SetServiceStatus(svc.ServiceHandle, ref serviceStatus);
            }

            internal ServiceStateEnum GetStatus { get => status; }

            public enum ServiceStateEnum {
                SERVICE_STOPPED = 0x00000001,
                SERVICE_START_PENDING = 0x00000002,
                SERVICE_STOP_PENDING = 0x00000003,
                SERVICE_RUNNING = 0x00000004,
                SERVICE_CONTINUE_PENDING = 0x00000005,
                SERVICE_PAUSE_PENDING = 0x00000006,
                SERVICE_PAUSED = 0x00000007,
            }

            [StructLayout(LayoutKind.Sequential)]
            public struct ServiceStatus {
                public long dwServiceType;
                public ServiceStateEnum dwCurrentState;
                public long dwControlsAccepted;
                public long dwWin32ExitCode;
                public long dwServiceSpecificExitCode;
                public long dwCheckPoint;
                public long dwWaitHint;
            };
        }
        
        #endregion
    }
}
