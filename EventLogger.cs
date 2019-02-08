#define LOG_TRACE

using System;
using System.Diagnostics;

namespace LSAssetDataService {

    public class EventLogger {
        
        #region VARIABLES & CONSTANTS -------------------------------------------------------------
        // constants
        private const string EVENTLogName = "LS Asset Data Service";

        // instance variables
        private System.Diagnostics.EventLog eventLog;
        private string eventLogSourceName;
        
        // public accessors
        public string EventSourceName { get => eventLogSourceName; }
        public string EventLogName { get => EVENTLogName; }
        #endregion

        #region INITIALISATION ---------------------------------------------------------------
        public EventLogger(EventLog eventLogReference, string eventSource) {

            // event source name is the enum stripped of underscores and converted to titlecase
            eventLogSourceName = eventSource;        // e.g. "Asset Workflow Data"
            
            //setup the event log 
            eventLog = eventLogReference;
            eventLog.Source = eventLogSourceName;
            eventLog.Log = EVENTLogName;
            eventLog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 0);

        }
        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------
        public void Info(string message, EventIdEnum id) {
            eventLog.WriteEntry(FormatLog(message, id), EventLogEntryType.Information, (int)id);
        }

        public void Warn(string message, EventIdEnum id) {
            eventLog.WriteEntry(FormatLog(message, id), EventLogEntryType.Warning, (int)id);
        }

        public void Error(string message, EventIdEnum id, Exception ex) {
            eventLog.WriteEntry(FormatLog(message, id) + Environment.NewLine 
                + ex.Source + Environment.NewLine
                + ex.StackTrace 
                , EventLogEntryType.Error, (int)id);
        }

        public void Trace(string message, EventIdEnum id) {
            #if LOG_TRACE
            eventLog.WriteEntry("[TRACE]" + Environment.NewLine + FormatLog(message, id), EventLogEntryType.Information, (int)id);
            # endif
        }

        // adds a message prefix depending on event id 
        private string FormatLog(string message, EventIdEnum id) {
            return Enum.GetName(typeof(EventIdEnum), id) + ": " + Environment.NewLine + message;
        }
        
        public enum EventIdEnum {
            INITIALISING = 1000,
            STARTING = 1001,
            RUNNING = 1002,
            CONTINUING = 1003,
            PAUSING = 1004,
            STOPPING = 1005,
            MONITORING = 2000,
            EXECUTING_SERVICE = 3000,
            QUERYING_DATA = 4000,
            STAGING_DATAFILE = 5000,
        }
#endregion
    }
}
