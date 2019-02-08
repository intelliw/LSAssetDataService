using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Drawing;
using OfficeOpenXml;
using OfficeOpenXml.Style;

namespace LSAssetDataService {

    /* exports asset data provided by the data extract into the shared folder used by Location Services to batch import the data. 
     * this class includes functions to create the output file, and to check the date and time of the last output 
     * data file. The data and time ('last modified') is used as criteria in the next data query executed by the data extract. 
     * The class also archives all files except the newest two files and move these into the archive subfolder. Only files with a 
     * prefix equal to <DataFilenamePrefix> are taken from the output folder and moved into the archive subfolder.
     */
   internal class FileWriter {

        #region VARIABLES & CONSTANTS -------------------------------------------------------------
        
        private const int DEFAULTLastModifiedDays  = 5;         // the default number of days to subtract from today's date, if target folder does not have any files in it, or for any reason the date-time substring in its filename cannot be parsed to get a Last Modified date.
        
        private string outputFolder;                            // output folder must exist (use script 'LSAssetDataService-Registry and Folders.ps1' 
        private EventLogger log;

        private const string iso8601DateTimeFormat = "dd/MM/yyyy HH:mm:ss.fff";

        #endregion

        #region INITIALISATION --------------------------------------------------------------------

        // CONSTRUCT filewriter with access to the target and archive folders for this service
        public FileWriter(string outputFolder, EventLogger logger) {

            this.outputFolder = outputFolder;
            log = logger;
        }
        #endregion

        #region PUBLIC - external interfaces ------------------------------------------------------

        /* checks the output folder and returns the date and time of the most recent file. The file's date and time is 
         * based on LastChanged field of the asset record which was most recently changed by Agility. 
         * This function returns a default of 5 days ago (specified by constant <defaultLastModifiedDays>) for the 
         * following conditions:
         * - If the folder is empty, or 
         * - the folder does not contain files with a prefix equal to <DataFilenamePrefix>, or 
         * - the filename suffix cannot be parsed as a date and time 
         * If a specific date is required for 'LastModified' you can change the date and time suffix of the most recent 
         * file in the target folder to the desired date and time. This approach may be used for the initial data load 
         * for example after release to production the file in the target folder can be named with a suitable date to load
         * all data from agility into CORE.
         * Ordering of files to find the newest file is based on filenames that are created by the data stage class 
         * and file are therefore assumed to be formatted consistently. (<DataFilenamePrefix>_YYYY_MM_DD_hh_mm) 
         * for example 'AssetDataFile_2017_08_10_10_01.csv' */
        internal DateTime RetrieveLatestModifiedFileTime() {
            
            const char SeparatorChar = '_';                  // the date-time substring has this seperator character 
            const int ZeroSeconds = 0;

            const int YYYY = 0;                              // position of each date time token in the substring 
            const int MM = 1;
            const int DD = 2;
            const int hh = 3;
            const int mm = 4;

            // set a default for 'last modified' - 5 days ago - which is returned if there are no files in the folder 
            DateTime lastMod = DateTime.Today.Subtract(new System.TimeSpan(DEFAULTLastModifiedDays, 0, 0, 0));

            try {

                // get the filtered list of files from the output folder
                string[] files = RetrieveDataFiles();

                int numFiles = files.Length;
                if (numFiles > 0) {

                    // get the latest file 
                    Array.Sort(files);                                                                  // sort the filenames so that the latest is last
                    string latestFile = files[numFiles-1].Substring(outputFolder.Length + 1);           // get name of last file which is the newest

                    // parse the file name and get the datetime substring from it
                    int prefixEnd = DataFilenamePrefix.Length;                                          // get the start of the data file prefix substring
                    int extensionStart = latestFile.IndexOf(".");                                       // get the file extension start point
                    string datetimeSubstring = latestFile.Substring(prefixEnd + 1, (extensionStart - 1) - prefixEnd);    // extract the date-time substring 
                     
                    // construct a last modified datetime based on substring tokens 
                    string[] token = datetimeSubstring.Split(SeparatorChar);                            // split into tokens 
                    lastMod = new DateTime(Int32.Parse(token[YYYY]),
                            Int32.Parse(token[MM]),
                            Int32.Parse(token[DD]),
                            Int32.Parse(token[hh]),
                            Int32.Parse(token[mm]),
                            ZeroSeconds);                                                               // there are no 'seconds' in the filename

                    // cleanup the file array
                    Array.Clear(files,0, files.Length); files = null;
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.STAGING_DATAFILE, ex);
            }
            return lastMod;
        }

        // counts the number of files in the output folder with the prefix and extension specified for this filewriter
        internal int RetrieveFileCount() {

            int numfiles = 0;
            try {
                // count the filtered list of files
                numfiles = RetrieveDataFiles().Length;

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.STAGING_DATAFILE, ex);
            }
            return numfiles; 
        }

        /* moves files with the prefix and extension specified for this filewriter, from the output folder into the 
         * archive folder. The newest 3 files (this number is specified in <FilesToRetainWhenArchiving>) are retained 
         * and the rest are archived. If a file with the same name exists in the destination it will be overwritten.
         * Note that the ordering of files to retain the newest is based on filename and it is 
         * assumed that all filenames are created by the data stage class and are therefore formatted consistently 
         * (<DataFilenamePrefix>_YYYY_MM_DD_hh_mm) for example 'e.g. AssetDataFile_2017_08_10_10_01.csv' */
        internal void ArchiveFiles() {

            try {
                // get the filtered list of files 
                string[] files = RetrieveDataFiles();
                int numFiles = files.Length;

                if (numFiles > FilesToRetainWhenArchiving) {

                    // archive all except the last 3 files
                    Array.Sort(files);                                                              // sort the files so that the latest is last
                    int fileNum = 0;
                    string currentFileName, targetFileSpec;
                    foreach (string currentFileSpec in files) {                     
                        fileNum++;                                                                  // count the files
                        if (fileNum <= (numFiles - FilesToRetainWhenArchiving)) {                   // skip and retain the last two files - move the rest into the archive folder
                            currentFileName = currentFileSpec.Substring(outputFolder.Length + 1);   // get the filename from the full filepath    
                            targetFileSpec = Path.Combine(ArchiveFolderPath, currentFileName);      // construct full path to the file
                            MoveFile(currentFileSpec, targetFileSpec);                              // move the file - delete file with same name if any exists in the destination 
                        }
                    }

                    // cleanup the file array
                    Array.Clear(files, 0, files.Length); files = null;
                                                                                                    // log details after archiving files  
                                                                                                    Log.Info("Archiving Files:"
                                                                                                        + Environment.NewLine + "\t" + (numFiles - FilesToRetainWhenArchiving) + " files archived, "
                                                                                                        + Environment.NewLine + "\t" + FilesToRetainWhenArchiving + " files retained"
                                                                                                        , EventLogger.EventIdEnum.STAGING_DATAFILE);
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.STAGING_DATAFILE, ex);
            }
        }

        // the path to the output folder - this folder must be cretaed before the service is installed and started
        internal string OutputFolderPath { get => outputFolder; }                   // create it using the provide install script 'LSAssetDataService-Registry and Folders.ps1'

        #endregion

        #region FINAL - cannot be overriden by subclass --------------------------------------------

        protected EventLogger Log { get => log; }                                   // sealed by not making the properties virtual

        // the file extension for the output file, based on DataFileType
        protected string DataFileExtension { get => Enum.GetName(typeof(FileTypeEnum), DataFileType).ToLower(); }

        // gets a list of files in the output folder, filtered for the data file prefix and file extension of this service
        protected string[] RetrieveDataFiles() { return Directory.GetFiles(outputFolder, DataFilenamePrefix + "*." + DataFileExtension); }

        // deletes the target file if it exists. The targetFileSpec must specify the full path and file name
        protected void DeleteFile(string targetFileSpec) {
            if (File.Exists(targetFileSpec)) {
                File.Delete(targetFileSpec);
            }
        }

        // moves the source file to the target. Both file spec arguments must specify the full path and file name
        protected void MoveFile(string sourceFileSpec, string targetFileSpec) {

            try { 
                Directory.CreateDirectory(Path.GetDirectoryName(targetFileSpec));          // create directory if needed 
                DeleteFile(targetFileSpec);                                                // delete file if one with the same name exists in the target 
                Directory.Move(sourceFileSpec, targetFileSpec);                            // move the file into the archive folder

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.STAGING_DATAFILE, ex);

            }
        }
        
        /* escapes special characters not compatible with CSV output:
         * replaces double quotes, commas, and carriage return/ newline chaacters */
        protected string EscapeCharacters(string value) {

            string separator = "";                          
            if (DataFileType == FileTypeEnum.CSV) {         // escapes needed for CSV 
                separator = ",";                            // set comma as the separator 
            }

            if (value.IndexOf("\"") >= 0) {                 // replace double quotes with 2 each 
                value = value.Replace("\"", "\"\"");
            }
            
            if (!String.IsNullOrEmpty(separator)) {         // e.g. if comma then escape, otherwise do nothing e.g. xlsx is happy with commas
                if (value.IndexOf(separator) >= 0) {        // if separator is present wrap the whole value in quotes
                    value = "\"" + value + "\"";
                }
            }

            while (value.Contains("\r")) {                  // replace carriage return characters
                value = value.Replace("\r", "");
            }
            while (value.Contains("\n")) {                  // replace new line return characters
                value = value.Replace("\n", "");
            }
            return value;
        }

        /* constructs the filename and path of the target file for this filewriter, based on the 
         * provided datetime, the data file prefix, and data file type. Returns the path and filename */
        protected string GetTargetFileName(DateTime timestamp) {
            const string fileNameDateSubstringFormat = "_yyyy_MM_dd_HH_mm";
            string targetFileName = DataFilenamePrefix + timestamp.ToString(fileNameDateSubstringFormat) + "." + DataFileExtension;  // e.g. AssetDataFile_2017_08_08_09_32.csv
            return targetFileName;
        }

        /* converts the rightmost characters in the asset code into a hexadecimal code suitable for 
         * RFID printing. Hex conversion is needed to convert the asset code into a print-compatible 
         * RFID identifier which is stored in the RFIDTagID column of the output file. The hex encoded 
         * number is mapped in the printer template and used to encode the  RFID tag label during printing. 
         * The 'bits' parameter specifies the total number of bits required in the returned identifer. 
         * Each character in the asset code requires 2 bits. The total number of bits includes a single character 
         * prefix, based on the parameter configured in the wirdows registry for this service (Parameters.RFIDCodePrefix). 
         * The prefix is environment-specific, for example 'U' for UAT or 'P' for PROD, to ensure that printed 
         * tags are kept separate in each environment. The returned encoding therefore consists of the hex encoded 
         * prefix and hex encoded characters in the asset code, which may be truncated as needed depending on the 
         * total number of bits specified by the caller. */
        internal string HexEncodeAsciiID(string asciiID, string asciiPrefix, int totalEncodedBits) {

            const int bitsPerAsciiChar = 2;
            const int firstLetter = 0;
            int numPrefixChars=0;

            StringBuilder outputCode = new StringBuilder();

            try {
                // encode prefix
                if (asciiPrefix.Length > 0) {                                                               // prefix can be a single character or blank  
                    numPrefixChars = 1;                                                                     // limit to a single character 
                    outputCode.Append(String.Format("{0:X}", Convert.ToInt32(asciiPrefix[firstLetter])));   // first encode the prefix 
                }

                // encode rightmost characters in asset code, depending on number of bits
                char[] asciiIDLetters = asciiID.ToCharArray();
                int maxLettersToEncode = (totalEncodedBits / bitsPerAsciiChar) - numPrefixChars;            // maximum number of chars to encode - if total encoded bits is 16 then: 8 minus 2 bits for the prefix = only 7 asset chars can be encoded
                int start = asciiIDLetters.Length > maxLettersToEncode ? 
                        asciiIDLetters.Length - maxLettersToEncode : 0;                                     // start is right-shifted, since we need the chars at end which are unique. if asset code is 8, start at char 1

                for (int letter = start; letter < asciiIDLetters.Length; letter++) { 
                    outputCode.Append(String.Format("{0:X}", Convert.ToInt32(asciiIDLetters[letter])));     // get the integer value of the letter and convert to hexadecimal
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.STAGING_DATAFILE, ex);
            }

            return outputCode.ToString();
        }
        #endregion

        #region VIRTUAL - must be overriden by subclass -------------------------------------------

        // the prefix for the output filename
        internal virtual string DataFilenamePrefix {
            get;
        }

        // the output file type - only csv and xlsx are supported
        internal virtual FileTypeEnum DataFileType {
            get;
        }
        
        // archive folder - must exist (create using script 'LSAssetDataService-Registry.ps1') 
        internal virtual string ArchiveFolderPath {
            get;
        }
        
        // the number of files to retain - the newest files are retained in the output folder, the rest are archived
        internal virtual int FilesToRetainWhenArchiving {
            get;
        }

        /* takes an asset data recordset with the headers and data needed for output and writes out to a file based 
         * on the DataFileType of this service subclass - either a CSV or XLSX. 
         * depending on the DataFileType of this service . 
         * The caller may optionally specify the target filespath and name. If npot provided a filename is created with a timestamp based on the 
         * last change date in the recordset (AssetDataRecordset.LastModified), and this datetime is returned to the caller. 
         * The asset recordset will have zero rows if there are no changed records. 
         * This method is invoked in the subclass by the service event (e.g LSAsetWorkflowData.OnService).
         * The subclass executes service specific functions then invokes the super to write out the recordset to disk.
         * If the file was written successfully the returned recordset will have Saved=true; 
         * If the target file exists and the optional overwrite argument was not specified as true, the file will not be overwritten 
         * and the recordset will be returned with Saved = false. 
         */
        internal virtual AssetDataRecordset WriteFile(AssetDataRecordset recordset, string targetFileSpec = "", bool overwrite = false) {

            try {
                // if a filespec was not provided by the caller, make the filespec based on the latest timestamp in the recordset
                if (targetFileSpec == "") { 
                    string targetFileName = GetTargetFileName(recordset.LastModified);
                    targetFileSpec = Path.Combine(OutputFolderPath, targetFileName);
                }

                if (recordset.Rows.Count == 0) {                                // skip save if the result does not include any data, because of skipped rows due to all rows failing validation above    
                    recordset.Saved = false;                                    // set to false as file could not be saved
                                                                                Log.Trace(this.GetType().Name + Environment.NewLine + "\tSkipped save: The validated recordset contains " + recordset.Rows.Count + " rows", EventLogger.EventIdEnum.STAGING_DATAFILE);


                } else if (File.Exists(targetFileSpec) && overwrite == false) { // if the file exists and optional overwrite was not specified, do not overwrite  
                    recordset.Saved = false;                                    // set to false as file could not be saved
                                                                                // log warning as file exists and we cannot save and overwrite
                                                                                Log.Warn("Could not write file as it exists. The data rows will be included in the next output file:"
                                                                                    + Environment.NewLine + "\t" + recordset.Rows.Count + " rows, " + recordset.Columns.Count + " columns"
                                                                                    + Environment.NewLine + "\t" + targetFileSpec
                                                                                    , EventLogger.EventIdEnum.STAGING_DATAFILE);

                } else {                                                        // if target file does nt exist go ahead and save it     
                    if (DataFileType == FileTypeEnum.CSV) {
                        WriteCSVFileToDisk(recordset, targetFileSpec);          // write CSV 

                    } else if (DataFileType == FileTypeEnum.XLSX) {
                        WriteXLSXFileToDisk(recordset, targetFileSpec);         // write XLSX
                    }
                    recordset.Saved = true;                                     // as there were no errors set Saved to signify that the file write was successful                                                                                
                                                                                // log details after writing the file  

                                                                                // dol not log for PAR Data which does ovewrites as it is a continous process and fills the log 
                                                                                if (!overwrite) {
                                                                                Log.Info("Creating File:"
                                                                                    + Environment.NewLine + "\t" + recordset.Rows.Count + " rows, " + recordset.Columns.Count + " columns"
                                                                                    + Environment.NewLine + "\tLast modified=" + recordset.LastModified.ToString(ISO8601DateTimeFormat)
                                                                                    + Environment.NewLine + "\t" + targetFileSpec
                                                                                    , EventLogger.EventIdEnum.STAGING_DATAFILE);
                                                                                }
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }

            return recordset;                                                   // returned without change 
        }

        #endregion

        #region PRIVATE - -------------------------------------------------------------------------
        
        // outputs all recrods in the AssetDataRecordset to a CSV file with a name and path as specified in targetaFileSpec
        private void WriteCSVFileToDisk (AssetDataRecordset recordset, string targetFileSpec) {

            const string csvSeparator = ",";
            StringBuilder csvRows = new StringBuilder();
            
            // get csv columns 
            int c = 0;
            foreach (string col in recordset.Columns) {
                c++;
                csvRows.Append(col);
                if (c < recordset.Columns.Count) { csvRows.Append(csvSeparator); }       // do not add a comma for the last column
            }
            csvRows.Append(Environment.NewLine);

            // get csv rows 
            foreach (AssetDataRow row in recordset.Rows) {
                c = 0;
                foreach (string field in row.Fields) {
                    c++;
                    csvRows.Append(field);
                    if (c < recordset.Columns.Count) { csvRows.Append(csvSeparator); }  // do not add a comma for the last column
                }
                csvRows.Append(Environment.NewLine);
            }

            // write the file
            File.WriteAllText(targetFileSpec, csvRows.ToString());

        }

        // outputs all recrods in the AssetDataRecordset to a CSV file with a name and path as specified in targetaFileSpec
        private void WriteXLSXFileToDisk(AssetDataRecordset recordset, string targetFileSpec) {

            try {
                using (ExcelPackage excel = new ExcelPackage()) {

                    // create the first worksheet
                    AddWorksheet(excel.Workbook,recordset);

                    // create the second worksheet if there is a supplementary recorset
                    if (recordset.SupplementaryData != null) {
                        AddWorksheet(excel.Workbook, recordset.SupplementaryData);
                    }

                    // write the file
                    excel.SaveAs(new FileInfo(targetFileSpec));
                }

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }
        }
        // creates a new worksheet and adds data from the recordset 
        private void AddWorksheet(ExcelWorkbook excelWorkbook, AssetDataRecordset recordset) {

            try {
                string worksheetName = recordset.Name;                                                          // get the tab name from the recordset's name    
                ExcelWorksheet worksheet = excelWorkbook.Worksheets.Add(worksheetName);                         // create the worksheet

                // create the header row
                int c = 0, r = 1;

                foreach (string col in recordset.Columns) {
                    c++;
                    worksheet.Cells[r, c].Value = col;                                                          // populate the header cell
                }

                // format the header cell 
                ExcelStyle style = worksheet.Cells[1, 1, 1, c].Style;
                style.Font.Bold = true;
                style.Font.Color.SetColor(System.Drawing.Color.White);
                style.Fill.PatternType = ExcelFillStyle.Solid;
                style.Fill.BackgroundColor.SetColor(Color.DarkBlue);

                // create the data rows 
                ExcelRange cell;
                foreach (AssetDataRow row in recordset.Rows) {
                    c = 0;
                    r++;                                                                                        // starts at row 2 after the header 
                    foreach (string field in row.Fields) {
                        c++;
                        cell = worksheet.Cells[r, c];

                        // populate each cell in the data row 
                        if (Double.TryParse(Convert.ToString(field), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out double numericValue)) {       // check if this is a number 
                            cell.Value = numericValue;                                                          // if so store it as a number ..to get rid of the little green triangle which appears in excel
                        } else {
                            cell.Value = field;                                                                 // store other values as strings          
                        }

                    }

                    // set a border for populated cells 
                    Border border = worksheet.Cells[r, 1, r, c].Style.Border;                                   // select the row 
                    border.Top.Style = border.Bottom.Style = border.Left.Style = border.Right.Style = ExcelBorderStyle.Thin;

                }

                // autofit columns 
                worksheet.Cells.AutoFitColumns();

            } catch (Exception ex) {
                Log.Error(ex.Message, EventLogger.EventIdEnum.QUERYING_DATA, ex);
            }


}

#endregion

#region INNER CLASSES & ENUMS -------------------------------------------------------------

// a list of file extensions for supported output files
internal enum FileTypeEnum {
            CSV = 1,
            XLSX = 2
        }

        #endregion

        #region STATIC ----------------------------------------------------------------------------

        public static string ISO8601DateTimeFormat => iso8601DateTimeFormat;

        #endregion
    }
}
