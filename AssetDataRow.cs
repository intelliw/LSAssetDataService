
using System;
using System.Collections.Generic;

/* the AssetDataRow contains data for one asset 
 */
namespace LSAssetDataService {
    internal class AssetDataRow {
        
        #region VARIABLES & CONSTANTS ------------------------------------------------------------- 

        private List<string> rowData = new List<string>();             // data for one asset in this recordset 
        #endregion

        #region PUBLIC - external interfaces -------------------------------------------------------

        public List<string> Fields { get => rowData; set => rowData = value; }
        #endregion
    }
}
