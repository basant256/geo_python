import Config as config
import arcpy
import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime
from datetime import date

start_time = time.time()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
def getCurrentFilePath():
    curDir = os.path.dirname(sys.argv[0])
    return os.path.abspath(curDir)

#get file directory name
mydir = getCurrentFilePath()

SdePath = mydir + config.sdeconnPath
globalFlag = True

CurrentTime = datetime.now().strftime("%Y%m%d-%H%M%S")
logDareTime = "_"+CurrentTime
logFilePath= mydir +"\\" +"Log"
arcpy.env.workspace = logFilePath
if not os.path.isdir(logFilePath):
     os.mkdir(logFilePath)
logCFullName =  logFilePath +"\\" +"AddBackhaulType"+ logDareTime +".log"
LogFileName = "AddBackhaulType"+ logDareTime

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

Logger = setup_logger(LogFileName,logCFullName)

Logger.info('Process Started')
Logger.info("LogPath {0}".format(logFilePath))

def write_log(msg):
    try:
        print( date.today().strftime("%d/%m/%Y") + " | " +datetime.now().time().strftime('%H:%M:%S')  + " | " + msg )
        Logger.info(msg)
        sys.stdout.flush()
    except Exception as e:
        Logger.exception(e)
        print( date.today().strftime("%d/%m/%Y") + " | " +datetime.now().time().strftime('%H:%M:%S')  + " | " + str(e) )
        sys.stdout.flush()
        sys.exit()
        globalFlag = False
def modify_length():
    globalFlag=True
    try:
      
        myconn= arcpy.ArcSDESQLExecute(SdePath )
        print("connected :" +SdePath)
        try:
        
            df = pd.read_excel(mydir + config.excelPath,usecols=[0,1])
            
            df = df.where(pd.notnull(df), None)
            print(df)
            df['Column Name'] = df.groupby(['Table Name'])['Column Name'].transform(lambda x : ','.join(x))
            # drop duplicate data
            df = df.drop_duplicates()
            #print(df)
            write_log("Length Modifying script Started.... ")
            for index, row in df.iterrows():
                tabe_name = row['Table Name'].split(".")[1]
                tabe_name = row['Table Name']
                in_field = row['Column Name']
                columnsArr = in_field.split(",")
                #print(tabe_name,"==",in_field)
                #print(columnsArr)
                if in_field:
                    inFeatures=SdePath +"\\"+ tabe_name
                   # inFeatures=SdePath +"\\NE.TELCO\\"+ row['Table  Name']
                    print("inFeatures :"+inFeatures) 
                    fieldName1=in_field
                    new_field_name=in_field
                    new_field_alias = in_field
                    fieldAlias=in_field
                    field_type="Text"
                    fieldlength="255"
                    field_is_nullable="NULLABLE"
                    clear_field_alias="false"

                    try:
                        #add new field in spatial table
                        arcpy.management.AddField(inFeatures, fieldName1, "TEXT",field_length=fieldlength,
                                  field_alias=fieldAlias, field_is_nullable="NULLABLE")
                        write_log("New Field Added.")
                    except Exception as err:
                        print("Add Field Error: ",err)
                        #logging.warning("Add Field Error:  {0}".format(err))
                        write_log("Add Field Error:  {0}".format(err))
                        globalFlag = False
        except Exception as err:
            print(err)
        
    except Exception as err:
        in_table = SdePath 
        #in_table = SdePath +'/NE.TELCO'
        write_log("Error in sde connection {0}".format(err))
        print("Database connection error: ",err)
        globalFlag = False   
    finally:
        print("Script Executed Successfully.")
        write_log("Script Executed Successfully.")
        write_log("Length Modifying script Ended.... ")
        return globalFlag

modify_length()