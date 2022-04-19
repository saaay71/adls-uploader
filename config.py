import requests
import json

azure_tenant_id = "AZURE_TENAT_ID"
store_name = "STORE_NAME" 


def sendToTeams(message):
 url = "WEBHOOK_URL"
 headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

 data = {"text": message}
 r = requests.post(url, data=json.dumps(data), headers=headers)
 
 return r
 

# I copied this part from somewhere and don't remember where :)
import enum
# Enum for size units
class SIZE_UNIT(enum.Enum):
   BYTES = 1
   KB = 2
   MB = 3
   GB = 4
    
def convert_unit(size_in_bytes, unit):
   """ Convert the size from bytes to other units like KB, MB or GB"""
   if unit == SIZE_UNIT.KB:
       return size_in_bytes/1024
   elif unit == SIZE_UNIT.MB:
       return size_in_bytes/(1024*1024)
   elif unit == SIZE_UNIT.GB:
       return size_in_bytes/(1024*1024*1024)
   else:
       return size_in_bytes