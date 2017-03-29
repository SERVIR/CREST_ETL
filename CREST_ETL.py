#-------------------------------------------------------------------------------
# Last Modified By: Githika Tondapu
# Major Modifcation Author: Kris Stanton
# Original Author: ESRI
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------
import arcpy
from arcpy import env
import os, sys, traceback, datetime, time, math, re
from datetime import timedelta
from os import path
import shutil, errno
from zipfile import ZipFile
import ftplib
import urllib
from urllib2 import URLError, HTTPError
import json
import boto
import ks_ConfigLoader
import ks_AdpatedLogger
import ks_CREST_postgresql_LineInputs
import pickle
g_theConfigSettings = ks_ConfigLoader.ks_ConfigLoader(r"D:\SERVIR\Scripts\CREST\config_CREST.xml")   # Live WestPrime version    # Instance of the global Settings object
g_theLogger = None
g_DetailedLogging_Setting = False

# Constants
sevInfo = 0
sevErr = 1
sevWarn = 2


#--------------------------------------------------------------------------
# Simple exception class to handle custom errors
#--------------------------------------------------------------------------
class CustomError(Exception):
    pass


#--------------------------------------------------------------------------
# Settings and Logger
#--------------------------------------------------------------------------
def get_Settings_Obj():
    Current_Config_Object = g_theConfigSettings.xmldict['ConfigObjectCollection']['ConfigObject']
    return Current_Config_Object

# Needed to prevent errors (while the 'printMsg' function is global...)
settingsObj = get_Settings_Obj()
# Logger Settings Vars
theLoggerOutputBasePath = settingsObj['Logger_Output_Location'] 
theLoggerPrefixVar = settingsObj['Logger_Prefix_Variable'] 
theLoggerNumOfDaysToStore = settingsObj['Logger_Num_Of_Days_To_Keep_Log'] 
# KS Mod, 2014-01   Adding a Script Logger 3        START
g_theLogger = ks_AdpatedLogger.ETLDebugLogger(theLoggerOutputBasePath, theLoggerPrefixVar+"_log", {

        "debug_log_archive_days":theLoggerNumOfDaysToStore
    })



# Print messages in Python and Esri GP environment
#   sev 0 = informational
#   sev 1 = error
#   sev 2 = warning
def printMsg(msg, sev = 0, detailedLoggingItem = False):

    global g_DetailedLogging_Setting
    if detailedLoggingItem == True:
        if g_DetailedLogging_Setting == True:
            # This configuration means we should record detailed log items.. so do nothing (code below executes as expected)
            pass
        else:
            # This config means we should NOT record detailed log items but one was passed in, so using 'return' to skip logging
            return

    textTimeLineWrapper = ""

    theSevVal = "sev("+ str(sev)+") msg: "
    theLogMsg = theSevVal + str(msg)
    textTimeLineWrapper += theLogMsg

    g_theLogger.updateDebugLog(textTimeLineWrapper)

    # Print to the console.
    print( "( " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f") + " ) : " + textTimeLineWrapper)

    if sev == 0:
        arcpy.AddMessage(msg)
    elif sev == 1:
        arcpy.AddError(msg)
    else:
        arcpy.AddWarning(msg)


# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % (seconds)
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)




#--------------------------------------------------------------------------
# ETL Indirect Support Methods (includes validation methods)
#--------------------------------------------------------------------------

# Gets and returns a list of files contained in the bucket and path.
#   Access Keys are required and are used for making a connection object.
def s3_GetFileListForPath(s3_AccessKey,s3_SecretKey,s3_BucketName, s3_PathToFiles, s3_Is_Use_Local_IAMRole):

    # Refactor for IAM Role
    s3_Connection = None
    if s3_Is_Use_Local_IAMRole == True:
        try:
            s3_Connection = boto.connect_s3(is_secure=False)
        except:
            s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey, is_secure=False)
    else:
        s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey, is_secure=False)
    s3_Bucket = s3_Connection.get_bucket(s3_BucketName,True,None)
    s3_ItemsList = list(s3_Bucket.list(s3_PathToFiles))
    retList = []
    for current_s3_Item in s3_ItemsList:
        retList.append(current_s3_Item.key)
    return retList

# Takes in a key and converts it to a URL.
def s3_Make_URL_From_Key(s3_BucketRootPath, current_s3_Key):
    # sample URL: https://bucket.servirglobal.net.s3.amazonaws.com//regions/africa/data/eodata/crest/TIFQPF2014021812.zip
    retString = str(s3_BucketRootPath) + str(current_s3_Key)
    return retString

# Gete the file name portion of an S3 Key Path
def get_FileNameOnly_From_S3_KeyPath(theS3KeyPath):
    retStr = theS3KeyPath.split('/')[-1]
    return retStr

# Get a list of all the files that are within our start and end dates
def get_List_Of_All_Files_Within_Start_And_End_Date(the_List_Of_All_Files, the_FileExtn, the_StartDateStr, the_EndDateStr ):
    list_Of_FileNames = []
    if the_FileExtn:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_List_Of_All_Files if f.endswith(the_FileExtn)]
    else:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_List_Of_All_Files]
    printMsg("get_List_Of_All_Files_Within_Start_And_End_Date: List of Filenames: " + str(list_Of_FileNames),sevInfo, True)

    retList = []
    if the_StartDateStr == the_EndDateStr:
        retList = [f for f in list_Of_FileNames if re.findall("\d+",f)[0] == the_StartDateStr]
    else:
        retList = [f for f in list_Of_FileNames if re.findall("\d+",f)[0] > the_StartDateStr and re.findall("\d+",f)[0] <= the_EndDateStr]

    printMsg("get_List_Of_All_Files_Within_Start_And_End_Date: List of Filenames within Date range: " + str(retList),sevInfo, True)
    return retList


# Check for expected CREST package Errors
def validate_Check_For_Crest_Error(theFileName):
	strloc = theFileName.find("_Q_Q")
	if strloc != -1:
		return True
	else:
		return False

# Check for CREST forecast type
def validate_Check_For_Crest_Forecast(theFileName):
	strloc = theFileName.find("_f_")
	if strloc != -1:
		return True
	else:
		return False

#--------------------------------------------------------------------------
# Extract Methods
#--------------------------------------------------------------------------

# Extract Raster from S3 list..
def extractRasterS3(extOptions,startDateStr,endDateStr,dateFmt, scratchWorkSpace, the_s3_Info, isTifFileGet):

    retObj = None
    timeStart_S3_Whole = time.time()

    counter_FilesDownloaded = 0
    fileDownloadLimiter = 40 # Number of files a single crest run will download.

    try:
        timeStart_GetS3Lists = time.time()

        # The extracted files
        extractedFilesDictList = []

        printMsg("    Starting s3 Extract Processes ",sevInfo) #!!!!!
        printMsg("   Date Variable, startDateStr: " + str(startDateStr),sevInfo, True)
        printMsg("   Date Variable, endDateStr: " + str(endDateStr),sevInfo, True)

        fileExtn = extOptions["extension"]
        # Get Whole List of file names. (ASC and TIF)
        s3_Is_Use_Local_IAM_Role = the_s3_Info['config_s3_isUseLocal_IAM_Role']
        s3AccessKey = the_s3_Info["config_s3_AccessKeyID"]
        s3SecretKey = the_s3_Info["config_s3_SecretAccessKey"]
        s3BucketName = the_s3_Info["config_s3_BucketName"]
        s3BucketRootPath = the_s3_Info["config_s3_BucketRootPath"]
        s3PathToASCFiles = the_s3_Info["config_s3_PathTo_ASC_Files"]
        s3PathToTIFFiles = the_s3_Info["config_s3_PathTo_TIF_Files"]

        # Are we getting Tif Files or Asc Files?
        theListOf_BucketPath_FileNames = None
        if isTifFileGet == True:
            theListOf_BucketPath_FileNames = s3_GetFileListForPath(s3AccessKey,s3SecretKey,s3BucketName,s3PathToTIFFiles, s3_Is_Use_Local_IAM_Role)			
        else:
            theListOf_BucketPath_FileNames = s3_GetFileListForPath(s3AccessKey,s3SecretKey,s3BucketName,s3PathToASCFiles, s3_Is_Use_Local_IAM_Role)


        # get a list of all the files within the start and end date
        filePaths_WithinRange = get_List_Of_All_Files_Within_Start_And_End_Date(theListOf_BucketPath_FileNames, fileExtn, startDateStr, endDateStr)

        printMsg("\n      filePaths_WithinRange: " + str(filePaths_WithinRange), sevInfo, True)
        printMsg("\n      Got lists of files to download from the S3 server " + " - " + timeElapsed(timeStart_GetS3Lists),sevInfo) #!!!!!

        # Detailed logging
        printMsg("   extractRasterS3: Param, s3AccessKey: " + str(s3AccessKey),sevInfo, True)
        printMsg("   extractRasterS3: Param, s3SecretKey: " + str(s3SecretKey),sevInfo, True)
        printMsg("   extractRasterS3: Param, s3BucketName: " + str(s3BucketName),sevInfo, True)
        printMsg("   extractRasterS3: Param, s3PathToASCFiles: " + str(s3PathToASCFiles),sevInfo, True)
        printMsg("   extractRasterS3: Param, s3PathToTIFFiles: " + str(s3PathToTIFFiles),sevInfo, True)

        printMsg("      Num of files to download: " + str(len(filePaths_WithinRange)),sevInfo) #!!!!!
        printMsg("   extractRasterS3: List of 'filePaths_WithinRange': " + str(filePaths_WithinRange),sevInfo, True)
        numFound = len(filePaths_WithinRange)
        if numFound == 0:
            if startDateStr == endDateStr:
                printMsg("      No files found for date string "+startDateStr,sevErr)
            else:
                printMsg("      No files found between "+startDateStr+" and "+endDateStr,sevErr)
            retObj = None
        else:

            # Iterate through each key file path and and perform the extraction.
            for s3_Key_file_Path_to_download in filePaths_WithinRange:

                if counter_FilesDownloaded < fileDownloadLimiter:

                    timeStart_CurrentFileDownload = time.time()
                    file_to_download = get_FileNameOnly_From_S3_KeyPath(s3_Key_file_Path_to_download)

                    # Get the date string from the downloaded file
                    dSTR = re.findall(r"\d+",file_to_download)[0]
                    downloadedFile = path.join(scratchWorkSpace,file_to_download)
                    currentURL_ToDownload = s3_Make_URL_From_Key(s3BucketRootPath, s3_Key_file_Path_to_download)
                    printMsg("   extractRasterS3: Variable, currentURL_ToDownload: " + str(currentURL_ToDownload),sevInfo, True)
                    printMsg("   extractRasterS3: Variable, downloadedFile: " + str(downloadedFile),sevInfo, True)

                    # Do the download stuff
                    theDLodaedURL = urllib.urlopen(currentURL_ToDownload)
                    open(downloadedFile,"wb").write(theDLodaedURL.read())
                    theDLodaedURL.close()
                    printMsg("      Downloaded File from, " + currentURL_ToDownload + " to location " + downloadedFile + " - " + timeElapsed(timeStart_CurrentFileDownload),sevInfo)

                    counter_FilesDownloaded += 1

                    # Extract the zipped file,
                    # Unzip the files if it's a ZIP file, return a list of the
                    #   extracted files
                    if str.upper(fileExtn) == "ZIP":
                        timeStart_Zip_Extraction = time.time()

                        try:
                            z = ZipFile(downloadedFile)
                            ZipFile.extractall(z,scratchWorkSpace)
                            zipFileList = [path.join(scratchWorkSpace,f) for f in z.namelist()]

                            printMsg("        Unzipped file " + path.basename(downloadedFile)+" - "+timeElapsed(timeStart_Zip_Extraction),sevInfo) #!!!!!

                            fileCounter = 0
                            for f in z.namelist(): #!!!!!
                                fileCounter = fileCounter + 1
                                printMsg("           "+f,sevInfo, True) #!!!!!
                            printMsg("        Unzipped, "+ str(fileCounter) + ", files from zip file " + path.basename(downloadedFile),sevInfo)

                            printMsg("      Adding Extracted file to list.  (date_string - single date, extracted_files - list of files in the zip) :    date_string: " + str(dSTR) + ", extracted_files (LIST): " + str(zipFileList),sevInfo)
                            extractedFilesDictList.append({"date_string":dSTR,"extracted_files":zipFileList})
                        except:
                            printMsg("        Error unzipping file",sevErr)
                        # Delete ZIP file after unzipping
                        try:
                            z.close()
                            os.remove(downloadedFile)
                            printMsg("        Deleted "+downloadedFile,sevInfo)
                        except:
                            printMsg("        Error deleting "+downloadedFile+"...please delete manually",sevWarn)
                    else:
                        extractedFilesDictList.append({"date_string":dSTR,"extracted_files":[downloadedFile]})
                else:
                    printMsg("        Max Number of files downloaded for this ETL run reached. ",sevInfo)

            # Set the return object to the extracted files list.
            retObj = extractedFilesDictList

        pass
    except:
        e = sys.exc_info()[0]
        printMsg("    S3 Error, Error Message: " + str(e), sevErr)

    printMsg("\n      s3 processes reached the end. " + " - " + timeElapsed(timeStart_S3_Whole),sevInfo)
    return retObj

#--------------------------------------------------------------------------
# Transform Methods
#--------------------------------------------------------------------------


# "Transform" files that are already in the right format and projection
#   (e.g., copy them from the scratch workspace to the raster output location)
#   Return a list of transformed files, their associated dataset, and
#   and primary date field
def transformRasterCopy(transOptions, varList,extFileList,dateSTR, rasterOutputLocation, the_S3_Info):
    printMsg("    transformRasterCopy: transformRasterCopy(params) was called ", sevInfo, True)
    try:
        printMsg("    transformRasterCopy: param: varList " + str(varList), sevInfo, True)
        printMsg("    transformRasterCopy: param: extFileList " + str(extFileList), sevInfo, True)
        printMsg("    transformRasterCopy: param: dateSTR " + str(dateSTR), sevInfo, True)
        printMsg("    transformRasterCopy: param: rasterOutputLocation " + str(rasterOutputLocation), sevInfo, True)
        printMsg("    transformRasterCopy: param: the_S3_Info " + str(the_S3_Info), sevInfo, True)
    except:
        printMsg("    transformRasterCopy: transformRasterCopy(params) could not output some of the params to the log.. ", sevInfo, True)

    try:
        coor_system = transOptions["coordinate_system"]

        # Transform data for each variable
        outputVarFileList = []
        for varDict in varList:

            varName = varDict["variable_name"]
            filePrefix = varDict["file_prefix"]
            fileSuffix = varDict["file_suffix"]
            mosaicName = varDict["mosaic_name"]
            primaryDateField = varDict["primary_date_field"]

            # Build the name of the raster file we're looking for based on
            #   the configuration for the variable and find it in the list
            #   of files that were extracted
            raster_base_name = filePrefix + dateSTR + fileSuffix
            printMsg("    DEBUG: BEFORE RENAME: raster_base_name " + raster_base_name, sevInfo, True)
            # Find the file in the list of downloaded files associated with the current variable
            raster_file = ""
            raster_name = ""
            for aName in extFileList:

                currBaseName = path.basename(aName)

                if currBaseName == raster_base_name:
                    raster_file = aName
                    raster_name = raster_base_name

            printMsg("    DEBUG: raster_file " + raster_file, sevInfo, True)

            # If we don't find the file in the list of downloaded files, skip this variable and move on; otherwise, process the file
            if len(raster_file) == 0:
                printMsg("    No file found for variable " + varName + "...skipping...", sevWarn)
            else:

                printMsg("    DEBUG: raster_name " + raster_name, sevInfo, True)
                # Add the output raster location for the full raster path
                out_raster = path.join(rasterOutputLocation, raster_name)
                printMsg("    DEBUG: out_raster " + out_raster, sevInfo, True)

                # Perform the actual conversion
                timeStart = time.time()
                arcpy.CopyRaster_management(raster_file, out_raster)    # This operation DOES overwrite an existing file (so forecast items get overwritten by actual items when this process happens)
                printMsg("    Copied "+path.basename(raster_file)+" to "+out_raster+" - "+timeElapsed(timeStart),sevInfo)

                # Define the coordinate system
                timeStart = time.time()
                sr = arcpy.SpatialReference(coor_system)
                arcpy.DefineProjection_management(out_raster, sr)
                printMsg("      Defined coordinate system "+sr.name+" - "+timeElapsed(timeStart),sevInfo)

                # Append the output file and it's associated variable to the list of files processed
                outputVarFileList.append({"raster_file":out_raster,"mosaic_ds_name":mosaicName,"primary_date_field":primaryDateField,"isForecast":False,"forecastDateStr":None})
            # Process forecast rasters if they exist.
            try:
                printMsg("    Checking for forecast files for this variable " + varName, sevInfo)
                forecast_Rasters_Info_List = []
                # Only process the files with a short suffix
                if len(fileSuffix) < 5:
                    # Get the forecast rasters
                    currDateFromString = datetime.datetime.strptime(dateSTR, "%Y%m%d%H")
                    daysAhead_1 = currDateFromString + datetime.timedelta(days = 1)
                    daysAhead_2 = currDateFromString + datetime.timedelta(days = 2)

                    # Expected 2 Datestrings to check against.
                    dateString_1_day = daysAhead_1.strftime("%Y%m%d%H")
                    dateString_2_day = daysAhead_2.strftime("%Y%m%d%H")

                    forecast_DateStrings =[]
                    forecast_DateStrings.append(dateString_1_day)
                    forecast_DateStrings.append(dateString_2_day)

                    # Iterate through the expected forecast datestrings
                    for current_Forecast_DateString in forecast_DateStrings:
                        # Iterate through the list of extracted files
                        for current_RasterFileName in extFileList:
                            # See if a forecast datestring is found
                            if current_RasterFileName.find(current_Forecast_DateString) != -1:
                                # Last check, make sure the item found matches the file prefix
                                if current_RasterFileName.find(filePrefix) != -1:
                                    # Raster found, add it to the list
                                    finalName = filePrefix + current_Forecast_DateString + fileSuffix # This uses the expected naming conventions that match the other items.. just with future dates.
                                    current_ForecastRasterInfo = {
                                        "Forecast_RasterFile_Location": current_RasterFileName,
                                        "Forecast_DateString": current_Forecast_DateString,
                                        "Forecast_Raster_Final_Filename":finalName
                                    }
                                    forecast_Rasters_Info_List.append(current_ForecastRasterInfo)

                if len(forecast_Rasters_Info_List) == 0:
                    printMsg("    No forecast files found for this variable " + varName + "...skipping...", sevInfo)
                else:
                    printMsg("    About to process, " + str(len(forecast_Rasters_Info_List)) + ", forecast items found for variable " + varName , sevInfo)
                    printMsg("    transformRasterCopy: Forecast Rasters Found: " + str(forecast_Rasters_Info_List) , sevInfo, True)
                    # Process the rasters found
                    for forecastRaster_Info in forecast_Rasters_Info_List:
                        # Peform the same operations for a normal raster here
                        current_F_Date = forecastRaster_Info["Forecast_DateString"]
                        current_F_RasterFileLocation = forecastRaster_Info["Forecast_RasterFile_Location"]
                        current_F_RasterFinalFileName = forecastRaster_Info["Forecast_Raster_Final_Filename"]

                        # Simillar process as found above for non forecast rasters
                        out_raster = path.join(rasterOutputLocation, current_F_RasterFinalFileName)
                        arcpy.CopyRaster_management(current_F_RasterFileLocation, out_raster)
                        sr = arcpy.SpatialReference(coor_system)
                        arcpy.DefineProjection_management(out_raster, sr)
                        outputVarFileList.append({"raster_file":out_raster,"mosaic_ds_name":mosaicName,"primary_date_field":primaryDateField,"isForecast":True,"forecastDateStr":current_F_Date})

            except:
                printMsg("      Error Processing forecast items! ",sevWarn)



        # Clean up temporary files
        printMsg("    Deleting temporary extracted files",sevInfo)
        for extFile in extFileList:
            # This override prevents text file deleting (they are needed in a later step)
            try:
                # KS Mod Feb 2014 Refactor for Adding CREST Line Items to Postgresql DB       START
                if extFile[-3:] == "txt":           # is the end of the file name 'txt'?
                    printMsg("Skipped Deleting Text file, " + extFile + ", saving for CREST LineItems Processing", sevInfo, True)
                    pass
                else:
                    os.remove(extFile)
                    printMsg("      Deleted "+extFile,sevInfo, True)
                # KS Mod Feb 2014 Refactor for Adding CREST Line Items to Postgresql DB       END
            except:
                printMsg("      Error deleting temporary extracted file: "+extFile+"...please delete manually",sevWarn)
        printMsg("transformRasterCopy: outputVarFileList: , " + str(outputVarFileList), sevInfo, True)
        return outputVarFileList

    # handle errors
    except:
        printMsg("    Transform error:\n"+str(arcpy.GetMessages(2)),sevErr)
        printMsg("    DEBUG: transformRasterCopy(params) ALERT(NILL, ERROR!!) ", sevInfo)
        return None



#--------------------------------------------------------------------------
# Load Methods
#--------------------------------------------------------------------------


# Load output raster(s) into mosaic dataset(s)
#   Return the number of rasters loaded
def loadRasterMosaicDataset(ldOptions,transFileList,mdWS):

    # Load each raster into its appropriate mosaic dataset
    numLoaded = 0
    for fileDict in transFileList:
        rasterFile = fileDict["raster_file"]
        rasterName = path.basename(rasterFile).split(".")[0]
        mosaicDSName = fileDict["mosaic_ds_name"]
        primaryDateField = fileDict["primary_date_field"]
        mosaicDS = path.join(mdWS, mosaicDSName)

        # For now, skip the file if the mosaic dataset doesn't exist.  Could
        #   be updated to create the mosaic dataset if it's missing
        if not arcpy.Exists(mosaicDS):
            printMsg("    Mosaic dataset "+mosaicDSName+" does not exist.  Skipping "+path.basename(rasterFile),sevWarn)
        else:
            try:
                # Add raster to mosaic dataset
                addError = False
                timeStart = time.time()
                arcpy.AddRastersToMosaicDataset_management(mosaicDS, "Raster Dataset", rasterFile,\
                                                           "UPDATE_CELL_SIZES", "UPDATE_BOUNDARY", "NO_OVERVIEWS",\
                                                           "2", "#", "#", "#", "#", "NO_SUBFOLDERS",\
                                                           "EXCLUDE_DUPLICATES", "BUILD_PYRAMIDS", "CALCULATE_STATISTICS",\
                                                           "NO_THUMBNAILS", "Add Raster Datasets","#")

                printMsg("    Added "+path.basename(rasterFile)+" to mosaic dataset "+mosaicDSName+" - "+timeElapsed(timeStart),sevInfo)

                # Debug Line to see full paths..
                printMsg("    loadRasterMosaicDataset: Added "+str(rasterFile)+" to mosaic dataset "+mosaicDSName+" - "+timeElapsed(timeStart),sevInfo, True)

                numLoaded = numLoaded + 1

            # Handle errors for AddRastersToMosaicDataset
            except:
                printMsg("    Error adding raster "+path.basename(rasterFile)+" to mosaic dataset "+mosaicDSName+"\n        "+str(arcpy.GetMessages(2)),sevErr)
                addError = True

            if not addError:
                # Calculate statistics on the mosaic dataset
                try:
                    timeStart = time.time()
                    arcpy.CalculateStatistics_management(mosaicDS,1,1,"#","SKIP_EXISTING","#")
                    printMsg("      Calculated statistics on mosaic dataset "+mosaicDSName+" - "+timeElapsed(timeStart),sevInfo)

                # Handle errors for calc statistics
                except:
                    printMsg("      Error calculating statistics on mosaic dataset "+mosaicDSName+"\n        "+str(arcpy.GetMessages(2)),sevWarn)

                # Update the configured attributes
                # Get the configured load options (attribute list)
                attrConfigList = ldOptions["attribute_list"]

                # Build attribute and value lists
                attrNameList = []
                attrExprList = []

                # Build a list of attribute names and expressions to use with
                #   the ArcPy Data Access Module cursor below
                for attrDict in attrConfigList:
                    attrName = attrDict["attr_name"]
                    attrExpr = attrDict["attr_expression"]

                    attrNameList.append(attrName)
                    attrExprList.append(attrExpr)

                # Update the attributes with their configured expressions
                #   (ArcPy Data Access Module UpdateCursor)
                try:
                    timeStart = time.time()
                    wClause = arcpy.AddFieldDelimiters(mosaicDS,"name")+" = '"+rasterName+"'"
                    with arcpy.da.UpdateCursor(mosaicDS, attrNameList, wClause) as cursor:
                        for row in cursor:
                            for idx in range(len(attrNameList)):
                                row[idx] = eval(attrExprList[idx])

                            cursor.updateRow(row)

                    printMsg("      Calculated attributes for raster - "+timeElapsed(timeStart),sevInfo)
                    del cursor

                # Handle errors for calculating attributes
                except:
                    printMsg("      Error calculating attributes for raster"+path.basename(rasterFile)+"\n        "+str(arcpy.GetMessages()),sevWarn)

    return numLoaded


#--------------------------------------------------------------------------
# Post ETL Methods
#--------------------------------------------------------------------------


# Remove old raster(s) from the mosaic dataset(s) and remove the files from
#   the file system if they get removed from the mosaic dataset
#   Return the number of rasters removed
def removeRastersMosaicDataset(varList,mdWS,oldDate,qryDateFmt):
    numRemoved = 0
    for varDict in varList:
        mosaicDSName = varDict["mosaic_name"]
        dateField = varDict["primary_date_field"]
        mosaicDS = path.join(mdWS, mosaicDSName)

        if not dateField:
            printMsg("      No primary date field defined for "+mosaicDSName+".  No rasters removed",sevWarn)
        else:
            timeStart = time.time()
            dstr = oldDate.strftime(qryDateFmt)

            query = dateField + " < '" + dstr + "'"

            try:
                # Remove the rasters from the mosaic dataset based on the query
                startCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))
                arcpy.RemoveRastersFromMosaicDataset_management(mosaicDS, query, "NO_BOUNDARY", "NO_MARK_OVERVIEW_ITEMS", \
                                                                "NO_DELETE_OVERVIEW_IMAGES", "NO_DELETE_ITEM_CACHE", \
                                                                "REMOVE_MOSAICDATASET_ITEMS", "NO_CELL_SIZES")
                endCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))

                printMsg("    Removed "+str(startCount-endCount)+" rasters ("+query+") from "+mosaicDSName+" - "+timeElapsed(timeStart),sevInfo)
                numRemoved = numRemoved + (startCount-endCount)

            # Handle errors for removing rasters
            except:
                printMsg("      Error removing rasters from "+mosaicDSName+"\n        "+str(arcpy.GetMessages()),sevErr)

    return numRemoved

# Cleans up old files from the output raster location (file system)
def dataCleanup(rasterOutputLocation,oldDate,dateFmt):
    numDeleted = 0

    arcpy.env.workspace = rasterOutputLocation

    oldDateStr = oldDate.strftime(dateFmt)
    oldDateInt = int(oldDateStr)

    try:
        for raster in arcpy.ListRasters("*", "All"):
            rastDateStr = re.findall(r"\d+",raster)[0]
            rastDateInt = int(rastDateStr)
            # KS Refactor..  if a delete operation fails, the code keeps on going and tries the next one....
            try:
                if(oldDateInt > rastDateInt):
                    arcpy.Delete_management(raster)
                    printMsg ("    Deleted "+raster,sevInfo)

                numDeleted = numDeleted + 1
            except:
                printMsg("  Error Deleting "+raster+"\n        "+str(arcpy.GetMessages()),sevErr)


    # Handle errors for deleting old raster files
    except:
        printMsg("      Error cleaning up old raster files from "+rasterOutputLocation+"\n        "+str(arcpy.GetMessages()),sevErr)

    return numDeleted

# Replacement for updateServices Function.. this one updates a single service.
def updateSingleService(service_Options):
    numUpdated = 0

    adminDirURL = service_Options["admin_dir_URL"]
    username = service_Options["username"]
    password = service_Options["password"]

    folderName = service_Options["folder_name"]
    serviceName = service_Options["service_name"]
    serviceType = service_Options["service_type"]

    printMsg("\n  Service To update: "+serviceName,sevInfo)

    # Map Servers Only
    try:
        # Get a token from the Administrator Directory
        timeStart = time.time()
        tokenParams = urllib.urlencode({"f":"json","username":username,"password":password,"client":"requestip"})
        tokenResponse = urllib.urlopen(adminDirURL+"/generateToken?",tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        if serviceType == "MapServer":
            timeStart = time.time()

            clearParams = urllib.urlencode({"token":token,"f":"json","folderName":folderName,"serviceName":serviceName,"type":serviceType})
            clearResponse = urllib.urlopen(adminDirURL+"/system/handlers/rest/cache/clear?",clearParams).read()
            clearResponseJSON = json.loads(clearResponse)
            clearStatus = clearResponseJSON["status"]

            if clearStatus == "success":
                printMsg("    Cleared REST cache for "+folderName+"/"+serviceName+"/"+serviceType+" - "+timeElapsed(timeStart),sevInfo)
                numUpdated = numUpdated + 1
            else:
                printMsg("    Unable to clear REST cache for "+folderName+"/"+serviceName+"/"+serviceType+" STATUS = "+clearStatus,sevErr)
        else:
            printMsg("    Service type "+serviceType+" not supported at this time",sevWarn)

    # Handle errors
    except HTTPError, e:
        print "HTTP Error:", e.code, url
    except URLError, e:
        print "URL Error:", e.reason, url

    return numUpdated

# Main Controller
# Wrapper for Main ManageTimeSeriesRaster Scripts.
class Generic_ETL_CREST_Main(object):
    '''
    '''
    def __init__(self):
        pass
        # Set Members

    # Private Members

    # Get oldest date based on a given interval string (e.g. 30 days)
    def _getOldestDate(self,intervalString,dateFormat):
        try:
            intervalValue = int(intervalString.split(" ")[0])
            intervalType = intervalString.split(" ")[1]

            deltaArgs = {intervalType:intervalValue}

            # Get the oldest date before now based on the interval and date format
            oldestDate = datetime.datetime.strptime(datetime.datetime.utcnow().strftime(dateFormat),dateFormat) - timedelta(**deltaArgs)
        except TypeError, e:
            printMsg("    Error getting oldest date: "+e.message,sevErr)
            return None

        return oldestDate

    # Main CREST Script Pipeline.
    def Run_Script_CREST(self):
        timeStartTotal = time.time()
        try:
            # Get Settings Object
            settingsObj = get_Settings_Obj()

            # Load the Various Settings into the application.
            config_Name = settingsObj['Name']                               
            config_Scratch_WorkSpace = settingsObj['Temporary_Scratch_Directory']         
            config_Mosaic_Dataset_WorkSpace = settingsObj['Mosaic_Dateset_Output_Directory']      
            config_Raster_Output_Location = settingsObj['Raster_Final_Output_Location']   
            config_Raster_Archive_Interval = settingsObj['Archive_Interval']             
            config_FileNameDateFormat = settingsObj['FileName_DateFormat']        
            config_QueryDateFormat = settingsObj['Query_DateFormat']               
            config_Extract_Type = settingsObj['Extract_Type']                     
            config_Extract_Range = settingsObj['Extract_Range']                    
            config_Extract_FileExtension = settingsObj['Extract_FileExtension']             
            config_Extract_FTP_ServerLocation = settingsObj['Extract_FTP_ServerLocation']  
            config_Extract_FTP_UserName = settingsObj['Extract_FTP_UserName']              
            config_Extract_FTP_Password = settingsObj['Extract_FTP_Password']               
            config_Extract_FTP_Directory = settingsObj['Extract_FTP_Directory']            

            # Logger Settings Vars
            theLoggerOutputBasePath = settingsObj['Logger_Output_Location'] # Folder where logger output is stored.
            theLoggerPrefixVar = settingsObj['Logger_Prefix_Variable'] # String that gets prepended to the name of the log file.
            theLoggerNumOfDaysToStore = settingsObj['Logger_Num_Of_Days_To_Keep_Log'] # Number of days to keep log


            # CREST Line Item Inputs
            pgdb_Host = settingsObj['CREST_pgdb_Host']            
            pgdb_DBName = settingsObj['CREST_pgdb_DBName']        
            pgdb_UserID = settingsObj['CREST_pgdb_UserID']        
            pgdb_UserPass = settingsObj['CREST_pgdb_UserPass']     

            # Amazon S3 info
            config_s3_isUseLocal_IAM_Role = settingsObj['s3_UseLocal_IAM_Role']
            config_s3_BucketName = settingsObj['s3_BucketName']
            config_s3_BucketRootPath = settingsObj['s3_BucketRootPath']
            config_s3_UserName = settingsObj['s3_UserName']
            config_s3_AccessKeyID = settingsObj['s3_AccessKeyID']
            config_s3_SecretAccessKey = settingsObj['s3_SecretAccessKey']
            config_s3_PathTo_ASC_Files = settingsObj['s3_PathTo_ASC_Files']
            config_s3_PathTo_TIF_Files = settingsObj['s3_PathTo_TIF_Files']


            bool_config_S3_Is_UseLocal_IAM_Role = True
            if config_s3_isUseLocal_IAM_Role == '1':
                bool_config_S3_Is_UseLocal_IAM_Role = True
            else:
                bool_config_S3_Is_UseLocal_IAM_Role = False


            s3_Info = {
                "config_s3_isUseLocal_IAM_Role":bool_config_S3_Is_UseLocal_IAM_Role,
                "config_s3_BucketName":config_s3_BucketName,
                "config_s3_BucketRootPath":config_s3_BucketRootPath,
                "config_s3_UserName":config_s3_UserName,
                "config_s3_AccessKeyID":config_s3_AccessKeyID,
                "config_s3_SecretAccessKey":config_s3_SecretAccessKey,
                "config_s3_PathTo_ASC_Files":config_s3_PathTo_ASC_Files,
                "config_s3_PathTo_TIF_Files":config_s3_PathTo_TIF_Files
            }


            # Tif Options
            config_Is_Download_And_Copy_TIF = settingsObj['Is_Download_And_Copy_TIF']
            bool_config_Is_Download_And_Copy_TIF = False
            if config_Is_Download_And_Copy_TIF == '1':
                bool_config_Is_Download_And_Copy_TIF = True
            else:
                bool_config_Is_Download_And_Copy_TIF = False

            # Detailed Line Item Query Data
            config_Is_GetQueryDetailLog = settingsObj['Is_Get_LineItem_QueryDetailLog']
            bool_config_Is_GetQueryDetailLog = False
            if config_Is_GetQueryDetailLog == '1':
                bool_config_Is_GetQueryDetailLog = True
            else:
                bool_config_Is_GetQueryDetailLog = False

            # Detailed Logging
            config_Is_Detailed_Logging = settingsObj['Is_Detailed_Logging']
            bool_config_Is_Detailed_Logging = False
            if config_Is_Detailed_Logging == '1':
                bool_config_Is_Detailed_Logging = True
            else:
                bool_config_Is_Detailed_Logging = False
            global g_DetailedLogging_Setting
            g_DetailedLogging_Setting = bool_config_Is_Detailed_Logging


            # First log entry of this script.   # Further messages are wrapped into the custom print function
            g_theLogger.updateDebugLog('======================================================================')
            g_theLogger.updateDebugLog(' Logger has been activated : Starting new run for : ' + str(config_Name))

            # Detailed logging message
            printMsg("Run_Script_CREST: Detailed Logging has been enabled !!!", sevInfo, True)

            # These are set based on the file extension of the unzipped (extracted) file from the source.
            the_normal_Suffix = ".asc"
            the_Q_Suffix = "_Q.asc"
            if bool_config_Is_Download_And_Copy_TIF == True:
                the_normal_Suffix = ".TIF"
                the_Q_Suffix = "_Q.TIF"


            # KS Refactor May 2014, Service and all of its variable names changed.. updating them now.. leaving the old working ones commented.
            # Variables and their options
            #   Supports different data types per variable, if needed
            variableDictList = [{"variable_name":"CREST_NRT_Runoff",
                                 "file_prefix":"GOVar_R_",
                                 "file_suffix":the_normal_Suffix,
                                 "data_type":"FLOAT",
                                 "mosaic_name": "CREST_NRT_Runoff",
                                 "primary_date_field":"timestamp",
                                 "service_dict_list":[{"folder_name":"Africa",
                                                       "service_name":"CREST_NRT_Runoff_esri",
                                                       "service_type":"ImageServer"}]},
                                {"variable_name":"CREST_NRT_SoilMoisture",
                                 "file_prefix":"GOVar_SM_",
                                 "file_suffix":the_normal_Suffix,
                                 "data_type":"FLOAT",
                                 "mosaic_name":"CREST_NRT_SoilMoisture",
                                 "primary_date_field":"timestamp",
                                 "service_dict_list":[{"folder_name":"Africa",
                                                       "service_name":"CREST_NRT_SoilMoisture_esri",
                                                       "service_type":"ImageServer"},
                                                      {"folder_name":"Africa",
                                                       "service_name":"CREST_NRT_SoilMoisture_esri",
                                                       "service_type":"MapServer"}]},
                                {"variable_name":"CREST_QPF_Rain",
                                 "file_prefix":"GOVar_Rain_",
                                 "file_suffix":the_normal_Suffix,
                                 "data_type":"FLOAT",
                                 "mosaic_name":"CREST_QPF_Rain",
                                 "primary_date_field":"timestamp",
                                 "service_dict_list":[{"folder_name":"Africa",
                                                       "service_name":"CREST_QPF_Rain_esri",
                                                       "service_type":"ImageServer"}]},
                                {"variable_name":"CREST_Quantile_Runoff", 
                                 "file_prefix":"GOVar_R_",
                                 "file_suffix":the_Q_Suffix,
                                 "data_type":"FLOAT",
                                 "mosaic_name":"CREST_Quantile_Runoff", 
                                 "primary_date_field":"timestamp",
                                 "service_dict_list":[{"folder_name":"Africa", 
                                                       "service_name":"CREST_Quantile_Runoff_esri",
                                                       "service_type":"ImageServer"}]},
                                {"variable_name":"CREST_Quantile_SoilMoisture", 
                                 "file_prefix":"GOVar_SM_",
                                 "file_suffix":the_Q_Suffix,
                                 "data_type":"FLOAT",
                                 "mosaic_name":"CREST_Quantile_SoilMoisture", 
                                 "primary_date_field":"timestamp",
                                 "service_dict_list":[{"folder_name":"Africa", 
                                                       "service_name":"CREST_Quantile_SoilMoisture_esri",
                                                       "service_type":"ImageServer"}]}]

            # Extract options
            # KS 2/2014 Config Refactor
            extractOptions = {
                "range":config_Extract_Range,                  
                "type":config_Extract_Type,                   
                "location":config_Extract_FTP_ServerLocation,  
                "user":config_Extract_FTP_UserName,             
                "pswd":config_Extract_FTP_Password,            
                "directory":config_Extract_FTP_Directory,      
                "extension":config_Extract_FileExtension       
            }
            transformOptionType = "ASCII"
            if bool_config_Is_Download_And_Copy_TIF == True:
                transformOptionType = "DOWNLOADANDCOPY"
            # KS Config, downloads and copies tif files.
            transformOptions = {
                "type":transformOptionType,
                "output_type":"tif",
                "coordinate_system":"WGS 1984"
            }
            # Load options
            loadOptions = {
                "type":"MOSAIC_DATASET",
                "attribute_list":[{"attr_name":"timestamp",
                                   "attr_expression":'datetime.datetime.strptime(re.findall(r"\d+",rasterName)[0],'+"'"+config_FileNameDateFormat+"')"}]
            }
            pkl_file = open('config.pkl', 'rb')
            myConfig = pickle.load(pkl_file)
            pkl_file.close()
            updateServiceOptions = {
                "admin_dir_URL":myConfig['admin_dir_URL'],
                "username":myConfig['username'],
                "password":myConfig['password']
            }

            # For updating a single service
            updateSingleServiceOptions = {
                "admin_dir_URL":myConfig['admin_dir_URL'],
                "username":myConfig['username'],
                "password":myConfig['password'],
                "folder_name":myConfig['folder_name'],
                "service_name":myConfig['service_name'], 
                "service_type":myConfig['service_type']
            }

            # Validate inputs
            printMsg("================================== VALIDATING CONFIGURATION ==================================",sevInfo)

            # Make sure the scratch workspace path exists, try to create it if it doesn't exist
            if not arcpy.Exists(config_Scratch_WorkSpace):
                try:
                    os.makedirs(config_Scratch_WorkSpace)
                    printMsg("    Created scratch workspace "+config_Scratch_WorkSpace,sevInfo)
                except:
                    msgError = "  Cannot create missing scratch workspace...aborting" + config_Scratch_WorkSpace
                    raise CustomError
            else:
                printMsg("    Validated scratch workspace",sevInfo) #!!!!!
                pass

            # Make sure the mosaic dataset workspace path exists and that it's an ArcGIS Workspace
            if not arcpy.Exists(config_Mosaic_Dataset_WorkSpace):
                msgError = "  Mosiac dataset workspace " + config_Mosaic_Dataset_WorkSpace + " does not exist...aborting"
                raise CustomError
            else:
                descWS = arcpy.Describe(config_Mosaic_Dataset_WorkSpace)
                if not descWS.dataType == "Workspace":
                    msgError = "  Path for the mosaic dataset workspace, " + config_Mosaic_Dataset_WorkSpace + " is not a valid workspace...aborting"
                    raise CustomError
                else:
                    printMsg("    Validated mosaic dataset workspace",sevInfo) #!!!!!				                    
                    pass

            # Make sure the output raster directory exists, try to create it if it doesn't exist
            if not arcpy.Exists(config_Raster_Output_Location):
                try:
                    os.makedirs(config_Raster_Output_Location)
                    printMsg("    Created output raster directory "+config_Raster_Output_Location,sevInfo)
                except:
                    msgError = "  Cannot create missing output raster directory " + config_Raster_Output_Location + "...aborting"
                    raise CustomError
            else:
                printMsg("    Validated output raster directory",sevInfo) #!!!!!
                pass

            timeStartTotal = time.time()
            env.overwriteOutput = True

            # Get the oldest date based on the archive interval (e.g. 30 days)
            oldestDate = self._getOldestDate(config_Raster_Archive_Interval,config_FileNameDateFormat)

            # Extract the data based on the configured extraction type (e.g. FTP, URL)
            extractTimeStart = time.time()

            printMsg("================================== EXTRACTING ==================================",sevInfo)
            extractError = False

            extractRange = extractOptions["range"]
            extractType = str.upper(extractOptions["type"])

            if extractRange == "LATEST":
                # Find the most recent date of raster processed for each variable
                #   Set the start date to the oldest of these dates (e.g., in case
                #   one variable is missing more files than others)
                startDate = None
                for varDict in variableDictList:
                    mosaicName = varDict["mosaic_name"]
                    primaryDateField = varDict["primary_date_field"]
                    mosaicDS = os.path.join(config_Mosaic_Dataset_WorkSpace,mosaicName)
                    sortedDates = sorted([row[0] for row in arcpy.da.SearchCursor(mosaicDS,primaryDateField)])
                    # KS message, What if the array has 0 elements?
                    printMsg("DEBUG: " + str(sortedDates),sevInfo, True)  # DEBUG LINE
                    try:
                        maxDate = sortedDates[-1]

                        if (not startDate) or (maxDate < startDate):
                            startDate = maxDate
                    except: 
                        startDate = datetime.datetime.now() + datetime.timedelta(-30)

                startDateString = startDate.strftime(config_FileNameDateFormat)
                endDateString = datetime.datetime.utcnow().strftime(config_FileNameDateFormat)
                printMsg("\n  PROCESSING LATEST MISSING FILES",sevInfo)

            elif extractRange == "ALL":
                startDateString = oldestDate.strftime(config_FileNameDateFormat)
                endDateString = datetime.datetime.utcnow().strftime(config_FileNameDateFormat)

                printMsg("\n  PROCESSING ALL FILES WITHIN ARCHIVE INTERVAL ("+config_Raster_Archive_Interval+")",sevInfo)

            else:
                startDateString = extractRange
                endDateString = extractRange
                printMsg("\n  PROCESSING FILE FOR SPECIFIC DATE STRING",sevInfo)

                # Check if entered date string is within the archive interval
                dateDate = datetime.datetime.strptime(startDateString,config_FileNameDateFormat)

                if dateDate < oldestDate:
                    msgError = "    Date string "+startDateString+" is outside the archive interval of "+config_Raster_Archive_Interval+"...aborting"
                    raise CustomError
                else:
                    printMsg("    Date string = " + startDateString,sevInfo) #!!!!!

            if extractType == "FTP":
                pass
            elif extractType == "URL":
                pass
            elif extractType == "S3":
                extractFileDictList = extractRasterS3(extractOptions,startDateString,endDateString,config_FileNameDateFormat,config_Scratch_WorkSpace,s3_Info, bool_config_Is_Download_And_Copy_TIF)
                pass
            else:
                msgError = "  Invalid extract type: " + extractType
                raise CustomError

            # If no data was extracted, inform the user and quit
            if not extractFileDictList:
                msgError = "  No data extracted"
                raise CustomError

            printMsg("  Time elapsed (EXTRACT): "+timeElapsed(extractTimeStart)+"...",sevInfo)

            # Transform the data based on the configured transformation type (e.g., ASCII, NO_TRANSFORMATION)
            transformTimeStart = time.time()

            printMsg("================================== TRANSFORMING ==================================",sevInfo)

            transformType = str.upper(transformOptions["type"])

            if transformType in ["ASCII", "DOWNLOADANDCOPY","NO_TRANSFORM"]:
                transformFileDictList = []
                for extractFileDict in extractFileDictList:
                    dateString = extractFileDict["date_string"]
                    extractFileList = extractFileDict["extracted_files"]

                    printMsg("\n  DATE STRING = "+dateString,sevInfo) #!!!!!

                    if transformType == "ASCII":
                        pass
                    elif transformType == "DOWNLOADANDCOPY":
                        # Get the list of tifs for this operation,
                        transformFileList = transformRasterCopy(transformOptions, variableDictList, extractFileList, dateString, config_Raster_Output_Location, s3_Info)
                    else: # NO_TRANSFORM
                        transformFileList = transformRasterCopy(transformOptions, variableDictList, extractFileList, dateString, config_Raster_Output_Location, s3_Info)

                    if not transformFileList:
                        printMsg("    No data transformed for date string "+dateString,sevWarn)
                    else:
                        # Group the transformed file lists into separate
                        #   dictionaries for each date string
                        transformFileDictList.append({"date_string":dateString,"transformed_files":transformFileList})
            else:
                msgError = "  Invalid transform type: " + transformType
                raise CustomError

            # If no data was transformed, inform the user and quit
            if len(transformFileDictList) == 0:
                msgError = "  No data transformed"
                raise CustomError
            printMsg("  Time elapsed (TRANSFORM): "+timeElapsed(transformTimeStart)+"...",sevInfo)

            # Load the raster(s) into the associated mosaic dataset(s), calculating configured attributes
            loadTimeStart = time.time()
            printMsg("================================== LOADING ==================================",sevInfo)
            loadType = str.upper(loadOptions["type"]) # Also used for removing later

            if loadType == "MOSAIC_DATASET":
                mosaicDictList = []
                for transformFileDict in transformFileDictList:
                    dateString = transformFileDict["date_string"]
                    transformFileList = transformFileDict["transformed_files"]

                    printMsg("\n  DATE STRING = "+dateString,sevInfo) #!!!!!

                    numberLoaded = loadRasterMosaicDataset(loadOptions, transformFileList, config_Mosaic_Dataset_WorkSpace)

                    if numberLoaded == 0:
                        printMsg("    No data loaded for date string "+dateString,sevWarn)
            else:
                msgError = "  Invalid load type: " + loadType
                raise CustomError

            printMsg("  Time elapsed (LOAD): "+timeElapsed(loadTimeStart)+"...",sevInfo)

            # Post ETL Operations Begin.
            printMsg("================================== Post ETL Operations ==================================", sevInfo)

            # KS Mod Feb 2014 Refactor for Adding CREST Line Items to Postgresql DB       START
			
            # Add Line Items to the postgresql database and Remove text files after.
            insertLineItemsTimeStart = time.time()
            printMsg("\nINSERTING CREST LINE ITEMS",sevInfo)

            # Define the folder name
            curr_15th_Hour_FolderName = config_Scratch_WorkSpace  # same as the settings folder  Setting: Temporary_Scratch_Directory
            printMsg("\nCREST LINE ITEMS Folder name: " + str(curr_15th_Hour_FolderName),sevInfo)

            try:
                # New Instance
                CREST_postgresql_LineInputs_Instance = ks_CREST_postgresql_LineInputs.CREST_postgresql_LineInputs(pgdb_Host, pgdb_DBName, pgdb_UserID, pgdb_UserPass)
                currentLineItemLogText = CREST_postgresql_LineInputs_Instance.perform_Postgresql_LineInputs(curr_15th_Hour_FolderName, bool_config_Is_GetQueryDetailLog)
                printMsg("\nCREST LINE ITEMS LOG OUTPUT: " + str(currentLineItemLogText),sevInfo)


            except:
                printMsg("\nERROR: CREST LINE ITEMS INPUT FAILED: " + str(currentLineItemLogText),sevErr)

            finally:
                printMsg("\nCREST LINE ITEMS Clean up, Removing txt files from folder: " + str(curr_15th_Hour_FolderName),sevInfo)
                try:
                    fileDeleteCounter = 0
                    # Get a list of all text files in the scratch folder and remove them.
                    filelist = [ currentTxtFile for currentTxtFile in os.listdir(curr_15th_Hour_FolderName) if currentTxtFile.endswith(".txt") ]
                    for currentTxtFile_T in filelist:
                        currFullPathToTxtFile = os.path.join(curr_15th_Hour_FolderName,currentTxtFile_T)
                        try:
                            os.remove(currFullPathToTxtFile)
                            fileDeleteCounter = fileDeleteCounter + 1
                            printMsg("      Deleted "+currFullPathToTxtFile,sevInfo, True)
                        except:
                            printMsg("      Error Deleting "+currFullPathToTxtFile+", DELETE THIS MANUALLY!",sevErr)
                    printMsg("      Deleted, "+ str(fileDeleteCounter) + ", scratch text files",sevInfo)
                except:
                    printMsg("      Error Getting file list to Delete text files from folder "+curr_15th_Hour_FolderName+", DELETE THE FILES IN THIS FOLDER MANUALLY!",sevErr)

                printMsg("\n",sevInfo)


            printMsg("  Time elapsed (INSERTING CREST LINE ITEMS): "+timeElapsed(insertLineItemsTimeStart)+"...",sevInfo)
            # KS Mod Feb 2014 Refactor for Adding CREST Line Items to Postgresql DB       END

            # Remove rasters from the mosaic dataset that are outside the configured archive interval
            removeRastersTimeStart = time.time()

            printMsg("\nREMOVING RASTERS OLDER THAN "+config_Raster_Archive_Interval,sevInfo)

            removeType = loadType # Use the same type configured for loading

            if removeType == "MOSAIC_DATASET":
                numberRemoved = removeRastersMosaicDataset(variableDictList,config_Mosaic_Dataset_WorkSpace,oldestDate,config_QueryDateFormat)
            else:
                msgError = "  Invalid remove type: " + removeType
                raise CustomError

            printMsg("  Time elapsed (REMOVE RASTERS): "+timeElapsed(removeRastersTimeStart)+"...",sevInfo)

            if numberRemoved > 0:
                # Clean up files from the output raster location that are outside the configured archive interval
                dataCleanupTimeStart = time.time()
                printMsg("\nCLEANING UP RASTER FILES OLDER THAN "+config_Raster_Archive_Interval,sevInfo)
                numberDeleted = dataCleanup(config_Raster_Output_Location,oldestDate,config_FileNameDateFormat)
                printMsg("    Deleted "+str(numberDeleted)+" raster files",sevInfo)
                printMsg("  Time elapsed (CLEANUP RASTER FILES): "+timeElapsed(dataCleanupTimeStart)+"...",sevInfo)

            # Update services associated with variables (if data was loaded or if rasters were removed)
            if (numberLoaded == 0) and (numberRemoved == 0):
                printMsg("\nSKIPPING UPDATE SERVICES (No data loaded and no rasters removed)",sevInfo)
            else:
                updateServicesTimeStart = time.time()
                printMsg("\nUPDATING SERVICES",sevInfo)
                numberUpdated = updateSingleService(updateSingleServiceOptions)
                printMsg("  Time elapsed (UPDATE SERVICES): "+timeElapsed(updateServicesTimeStart)+"...",sevInfo)
            # Clean old Log files
            printMsg("\nRemoving outdated log files",sevInfo)
            try:
                g_theLogger.deleteOutdatedDebugLogs()
                printMsg("\nOutdated log files now removed.",sevInfo)
            except:
                printMsg("\nError Removing outdated log files",sevInfo)

        except CustomError:
            printMsg(msgError,sevErr)

        except arcpy.ExecuteError:
            # Get the geoprocessing error messages
            msgs = arcpy.GetMessage(0)
            msgs += arcpy.GetMessages(2)

            # Display arcpy error messages
            printMsg("Error in SERVIR_ManageTimeSeriesRaster",sevWarn)
            printMsg(msgs,sevErr)

        except Exception, ex:
            # Get the traceback object
            import traceback, sys
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning the error into a message string
            pymsg = tbinfo + "\n" + str(sys.exc_type)+ ": " + str(sys.exc_value)

            # Display arcpy error messages
            printMsg("Error in SERVIR_ManageTimeSeriesRaster",sevWarn)
            printMsg(pymsg,sevErr)

        finally:
            printMsg("\nProcess Completed",sevInfo)
            printMsg("Total time elapsed: "+timeElapsed(timeStartTotal)+"...",sevInfo)

# Execution entry point
mainScriptInstance = Generic_ETL_CREST_Main()
mainScriptInstance.Run_Script_CREST()



