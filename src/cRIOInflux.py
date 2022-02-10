#!/usr/bin/env python3

from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time
import os,sys
from dotenv import load_dotenv, find_dotenv
import json

import epics
from epics import caget
from InfluxClientClass import InfluxClient


#InfluxDB setup:
dbConfig = load_dotenv(find_dotenv(".env"))
dbClient = InfluxClient(os.environ.get("URL"), os.environ.get("TOKEN"), os.environ.get("ORG"))
dbClientDet = dbClient.connect()
bucket = os.environ.get("BUCKET")

# Globals
DetTempValues=[]
valvesTempValues=[]

folder = os.path.dirname(__file__)
file_path = os.path.join(folder, 'CrioChnls.json')
with open(file_path,"r") as cRio_conf:
  cRio_json=json.load(cRio_conf)
  print (cRio_json)
chnlsCreated = False
def main():

  global chnlsCreated
  global Chnls2Monitor
  getData = True
  while (True):
    getData = False
    if not chnlsCreated:
        crio=cRio_json["cntrllrs"]["LN2"]["chosen_cRio"]
        chnls2Monitor=cRio_json["PV_channels"]
        pvs = [ epics.PV( cRio_json["cntrllrs"]["LN2"][str(crio)]+":"+chnls2Monitor[key] ) for key in chnls2Monitor]
        chnlsCreated = True

    try:
      # I know there are only two channels for now - It will be modified
      (DetTempValues, valvesTempValues) = [pv.get() for pv in pvs]
      print(DetTempValues[:])
      print(valvesTempValues[:])

    except Exception as e: # work on python 3.x
      print('Failed to caget: '+ str(e))
      chnlsCreated = False
      detTemperatures=f"{('Data N/A, at ' + datetime.now().strftime('%H:%M:%S')):^120}"
      valvesTemperatures=f"{'----------------,   ':^120}"
    
    timeset=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    write_api = dbClientDet.write_api(write_options=SYNCHRONOUS)
    
    p = Point("CoolingSystemValves")\
        .tag("Temp", "valves")\
        .field("temp1", valvesTempValues[0])\
        .field("temp2", valvesTempValues[1])\
        .field("temp3", valvesTempValues[2])\
        .field("temp4", valvesTempValues[3])\
        .field("temp5", valvesTempValues[4])\
        .field("temp6", valvesTempValues[5])\
        .field("temp7", valvesTempValues[6])\
        .field("temp8", valvesTempValues[7])\
        .field("temp9", valvesTempValues[8])\
        .field("temp10", valvesTempValues[9])\
        .time(timeset)
    #write_api.write(bucket=bucket, record=p)
    result=dbClient.writeData(bucket, p)
    print("DB insertion: ", result)

    p = Point("CoolingSystemDet")\
        .tag("Temp", "detectors")\
        .field("temp1", DetTempValues[0])\
        .field("temp2", DetTempValues[1])\
        .field("temp3", DetTempValues[2])\
        .field("temp4", DetTempValues[3])\
        .field("temp5", DetTempValues[4])\
        .field("temp6", DetTempValues[5])\
        .field("temp7", DetTempValues[6])\
        .field("temp8", DetTempValues[7])\
        .time(timeset)
    result=dbClient.writeData(bucket, p)
    
    query = 'from(bucket: "my-pc01-bucket")\
            |> range(start: -1h)\
            |> filter(fn: (r) => r["Temp"] == "detectors")\
            |> filter(fn: (r) => r["_field"] == "temp1")\
            |> yield(name: "last")'
    org = os.environ.get("ORG")
    results = dbClient.readData(org, query)
    
    print (results)
      
    time.sleep(10)
  dbClientDet.close()

if __name__ == '__main__':
  try:
    main()
  except(KeyboardInterrupt, SystemExit):
        print('Quitting ...')
        # Closing Db 
        dbClientDet.close()
        sys.exit()