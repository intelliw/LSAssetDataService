using System;
using System.Linq;

namespace LSAssetDataService {
       /* The service start command and parameters are written to the registry by the service installer.
                   "Computer\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\LSAssetDataService"
                   "ImagePath" = "C:\_frg\vs\repos\MyNewService\MyNewService\bin\Debug\MyNewService.exe" "<param 1>" .. "<param n>"< 
       Parameters may be changed in the registry after installation. The service reads these at startup:
                    parameter #1 - Interval minutes.                   (DEFAULT_INTERVAL_MINUTES)
                    parameter #2 - Interval seconds.                   (DEFAULT_INTERVAL_SECONDS)
                           The total interval includes both minutes and seconds. 
                           The LS asset data service will execute and repeat at this interval.
                               Interval minutes must be a number between 0 and 1440 (i.e. 24 hrs)
                               Interval seconds must be a number between 0 and 59.
                           Must be a number between 0 and > 59. 
                    parameter #3 - Name of CORE Service to monitor.    (CORE_RTLS_SVC_NAME)
                           The LS asset data service is slaved to this CORE service.
                           It monitors this CORE service to detect server failovers and restarts.
                               If Running the server is considered active and LS asset data service will execute.
                               If not Running the server is considered passive and LS asset data service will remain silent. 
                    parameter #4 - Target folder                       (TARGET_FOLDER)
                           The shared folder for csv/xlsx file staging (as configured in CORE). Directory separators must be escaped with '\'
       If these parameters are edited and fail validation the following defaults are used */
    public class ServiceParameters {

        #region VARIABLES & CONSTANTS -------------------------------------------------------------

        private EventLogger log;

        // parameter positions in the command line read from the registry when service is first started
        private const int MINUTESParameter = 0;
        private const int SECONDSParameter = 1;
        private const int MASTERSERVICEParameter = 2;
        private const int TARGETDIRParameter = 3;
        private const int DBSERVERParameter = 4;
        private const int ASSETDbNameParameter = 5;
        private const int LSDbNameParameter = 6;
        private const int RFIDPREFIXParameter = 7;

        // default parameters, used during  installation
        private const double DEFAULTMinutes = 0;                                           // default interval minutes
        private const double DEFAULTSeconds = 20;                                          // default interval seconds
        private const string DEFAULTMasterService = "CORE RFID Tag Stream Win Service";    // CORE Service to monitor active/ passive
        private const string DEFAULTTargetDir = "\\\\lxio006fil\\Apps\\Data\\AppUNC\\LocationSRV\\PCH\\UAT\\AssetDataFile";     // Target folder (must escape separators with '\'), each service must modify this when deploying the service
        public const string DEFAULTDatabaseServer = "wsc901usql";                          // passed to the DataReader constructor when instantiated by each service class, the dbserver is different for each environment 
        public const string DEFAULTAssetDatabaseName = "PCH_Agility_UAT";                  // usually the Agility database name. Passed to the DataReader constructor when instantiated by each service class, the db is different for each environment 
        public const string DEFAULTLSDatabaseName = "ECSGCore";                            // the CORE database name. Passed to the DataReader constructor when instantiated by each service class, the db is different for each environment 
        public const string DEFAULTRFIDCodePrefix = "";                                    // no prefix for PROD, U for UAT. The first two digits of the rfid code are hex encoded when printing labels, with this environment-specific character
        
        // instance variables are defaulted for the initial project installer, these are overwritten in constructor if an argument was provided     
        double intervalMinutes = DEFAULTMinutes;
        double intervalSeconds = DEFAULTSeconds;
        double intervalMilliseconds;                                            // defaulted in constructor
        string masterServiceName = DEFAULTMasterService;
        string targetFolderPath = DEFAULTTargetDir;
        string dbServer = DEFAULTDatabaseServer;                                
        string assetDb = DEFAULTAssetDatabaseName;
        string lsDb = DEFAULTLSDatabaseName;
        string rfidCodePrefix = DEFAULTRFIDCodePrefix;                        

        // public accessors
        public double IntervalMinutes { get => intervalMinutes; }
        public double IntervalSeconds { get => intervalSeconds; }
        public double IntervalMilliseconds { get => intervalMilliseconds; }     // total interval in milliseconds
        public string TargetFolderPath { get => targetFolderPath; }
        public string MasterServiceName { get => masterServiceName; }
        public string DatabaseServer { get => dbServer; }                       // passed to the DataReader constructor when instantiated by each service class, the dbserver is different for each environment 
        public string AssetDatabaseName { get => assetDb; }                     // passed to the DataReader constructor when instantiated by each service class, the db is different for each environment 
        public string LSDatabaseName { get => lsDb; }                           // CORE database name (e.g ECSGCore)
        public string RFIDCodePrefix { get => rfidCodePrefix; }                 // P for PROD, U for UAT. The first two digits of the rfid code are hex encoded when printing labels, with this environment-specific character

        #endregion

        #region INITIALISATION ----------------------------------------------------------------

        // if class is instantiated without arguments it will provide default parameters used for installation
        public ServiceParameters() {

            // read default interval - the rest are defaulted in constant declarations above
            CalculateTotalInterval();

        }
        
        /* constructor to use when service is running, constructs class with arguments read in from registry
         *  key: HKLM:\SYSTEM\CurrentControlSet\Services\LSAssetDataService
         *  attribute: ImagePath  */
        public ServiceParameters(string[] args, EventLogger logger) {

            log = logger;

            LoadServiceConfiguration(args);
        }
        #endregion

        #region PRIVATE - internal functions -------------------------------------------------------

        private void LoadServiceConfiguration(string[] args) {

            try {
                // read interval parameters 
                if (args.Count() > MINUTESParameter) {                                                              // arg # 0 - minutes
                    double.TryParse(args[MINUTESParameter], out intervalMinutes);
                    if (args.Count() > SECONDSParameter) {                                                          // arg # 1 - seconds
                        double.TryParse(args[SECONDSParameter], out intervalSeconds);
                    }
                }
                CalculateTotalInterval();

                // read master service parameter                                                                    // default has already been assigned (targetFolderPath = DEFAULTTargetDir) in declaration above 
                if (args.Count() > MASTERSERVICEParameter) { masterServiceName = args[MASTERSERVICEParameter]; }    // arg # 2 - seconds

                // read target directory path parameter                                                             // has already been assigned targetFolderPath = DEFAULTTargetDir in declaration above 
                if (args.Count() > TARGETDIRParameter) { targetFolderPath = args[TARGETDIRParameter]; }             // arg # 3 - target directory

                // read database parameters 
                if (args.Count() > DBSERVERParameter) { dbServer = args[DBSERVERParameter]; }                       // arg # 4 - database server - has already been assigned dbServer = DEFAULTDatabaseServer in declaration above 
                if (args.Count() > ASSETDbNameParameter) { assetDb = args[ASSETDbNameParameter]; }                  // arg # 5 - asset database name - default assigned in declaration above
                if (args.Count() > LSDbNameParameter) { lsDb = args[LSDbNameParameter]; }                           // arg # 6 - CORE database name - default assigned in declaration above

                // read RFID encoding prefix                                                                        // must be a single character. has already been assigned rfidCodePrefix = DEFAULTRFIDCodePrefix in declaration above
                if (args.Count() > RFIDPREFIXParameter) {
                    rfidCodePrefix = args[RFIDPREFIXParameter].Trim();                                              // arg # 7 - rfid prefix  
                    if (rfidCodePrefix.Length > 1) { rfidCodePrefix = rfidCodePrefix.Substring(0, 1); }             // limit to 1 character (or blank)
                }
            
            } catch (Exception ex) {
                log.Error(ex.Message, EventLogger.EventIdEnum.INITIALISING, ex);
            }

        }

        // validates minute and second intervals and calculates total interval from these in milliseconds. if validations fail use defaults 
        private void CalculateTotalInterval() {

            const int MaxMinutes24Hrs = 1440;
            const int MaxSeconds1Min = 59;
            
            // convert minutes and seconds to milliseconds
            if (intervalMinutes >= 0 && intervalMinutes <= MaxMinutes24Hrs) {
                intervalMilliseconds += (intervalMinutes * 60 * 1000);
            }
            if (intervalSeconds >= 0 && intervalSeconds <= MaxSeconds1Min) {
                intervalMilliseconds += (intervalSeconds * 1000);
            }

            // if 0 use default seconds interval as a fallback 
            if (intervalMilliseconds == 0) {
                intervalSeconds = DEFAULTSeconds;
                intervalMilliseconds += (intervalSeconds * 1000);
            }
        }
        #endregion
    }
}
