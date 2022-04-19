from azure.datalake.store import core, lib
import config

import sys, io
import schedule, threading, time

from datetime import datetime

from os import listdir
from os.path import isfile, join


import glob


def run_once_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()
    return schedule.CancelJob

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()
    

local_upload_folder_path = "LOCAL_FOLDER_PATH"
adls_upload_folder_path = "ADLS_FOLDER_PATH"


orginal_stdout = sys.stdout

buf = io.StringIO()
sys.stdout = buf
adlCreds = -1

uploaded_files = False

def postToTeams():
 output = buf.getvalue()
 if output == "":
  return
 orginal_stdout.write(output)

  
 now = datetime.now()
 current_time = now.strftime("%H:%M:%S")
 
 config.sendToTeams("{}<br>{}".format(current_time, output))
 
 buf.truncate(0)
 buf.seek(0)
 
def authenticate():
 global adlCreds
 adlCreds = lib.auth(config.azure_tenant_id)


def authenticated():
 if adlCreds ==  -1:
  return
  
#  print("Authentication sucess!")
  
 run_once_threaded(upload_files)
 
 return schedule.CancelJob

 
def upload_files():
 adl = core.AzureDLFileSystem(adlCreds, store_name=config.store_name)
 uploadedFolders = adl.ls(adls_upload_folder_path)
 
 uploadedFolders = set([folder.replace(adls_upload_folder_path[1:], "")+"/" for folder in uploadedFolders])
 
 local_folders = glob.glob(local_upload_folder_path+"*") # * means all if need specific format then *.csv
 local_folders = set([d.replace(local_upload_folder_path, "")+"/" for d in local_folders])

 to_upload_folders = local_folders.difference(uploadedFolders)

 folder_names = sorted([d.replace(local_upload_folder_path, "") for d in to_upload_folders])

 files = []
 for folder in folder_names:
  path = local_upload_folder_path+folder
  for f in listdir(path):
   if isfile(join(path, f)):
    files.append(folder+f)


 print("Uploading the following folders:<br>{}<br>Total number of files to upload:<br>{}".format(", ". join(folder_names), len(files)))
 

 for f in files:
  adl.put(local_upload_folder_path+f, adls_upload_folder_path+f)


 print("Upload finished.")
 time.sleep(2)
 global uploaded_files
 uploaded_files = True


def exit_program():
 if uploaded_files == True:
  exit()

schedule.every(2).seconds.do(run_threaded, postToTeams)
schedule.every().seconds.do(run_once_threaded, authenticate)
schedule.every().seconds.do(authenticated)
schedule.every().seconds.do(exit_program)


while 1:
    schedule.run_pending()
    time.sleep(1) 
 
 
 
 
 
 
 
 
 
 