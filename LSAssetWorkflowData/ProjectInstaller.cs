using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ServiceProcess;
using System.Configuration.Install;

namespace LSAssetDataService {
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer {

        public ProjectInstaller() {
            InitializeComponent();
        }

       
        protected override void OnBeforeInstall(IDictionary savedState) {

            ServiceParameters parameters = new ServiceParameters();   // provides defaults if constructed without args

            string imagePathParameters = parameters.IntervalMinutes + "\" \""
                + parameters.IntervalSeconds + "\" \""
                + parameters.MasterServiceName + "\" \""
                + parameters.TargetFolderPath + "\" \""
                + parameters.DatabaseServer + "\" \""
                + parameters.AssetDatabaseName + "\" \""
                + parameters.LSDatabaseName + "\" \""
                + parameters.RFIDCodePrefix;

            Context.Parameters["assemblypath"] = "\"" + Context.Parameters["assemblypath"] + "\" \"" + imagePathParameters + "\"";
            base.OnBeforeInstall(savedState);
        }
        
    }
}
