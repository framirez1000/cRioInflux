from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxClient:
    def __init__(self,url,token,org): 
        self.url=url 
        self.token = token
        self.org = org
    def connect(self):
        self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        return self._client
    def writeData(self, bucket, data):
        write_api = self._client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket, record=data)
        return 1
    def readData(self, org, query):
        query_api = self._client.query_api()
        result = query_api.query( org=org, query=query)
        returnData = []
        for table in result:
            for record in table.records:
                returnData.append((record.get_value(), record.get_field()))
        return returnData
    def close(self):
        self._client.close()