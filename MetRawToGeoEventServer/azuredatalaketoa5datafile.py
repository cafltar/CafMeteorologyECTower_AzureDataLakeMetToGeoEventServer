# -*- coding: utf-8 -*-
"""
Created on 06/27/2020

@author: bryan.carlson
"""
import pandas as pd
import arrow
import json
from . import datafile

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
import azure.identity

import io
import uri

class AzureDataLakeTOA5DataFile(datafile.DataFile):
    __stationId = ""

    def __readFileFromDataLake(self, blobPath):
        # Todo: Put in config file
        storage_account_name = self.config["datalakeInfo"]["storageAccountName"]
        file_system_name = self.config["datalakeInfo"]["fileSystemName"]
        client_id = self.config["datalakeInfo"]["clientId"]
        tenant_id = self.config["datalakeInfo"]["tenantId"]
        client_secret = self.config["datalakeInfo"]["clientSecret"]

        try:
            credential = azure.identity.ClientSecretCredential(tenant_id, client_id, client_secret)

            service_client = DataLakeServiceClient(account_url = "{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential = credential)

            file_system_client = service_client.get_file_system_client(file_system_name)

            file_client = file_system_client.get_file_client(blobPath)

            downloaded = file_client.download_file()
            downloaded_bytes = downloaded.readall()

            return downloaded_bytes

        except Exception as e:
            print(e)


    def __extractData(self, bloburi):
        blobPath = str(uri.URI(bloburi).path).replace('/raw/', '')
        fileio = self.__readFileFromDataLake(blobPath)
        # taa5 format can be extracted following:
        # skip row 0, make row 1 the header, skip rows 2 and 3 (metadata)
        station_id_str = {self.config['postDataMap']['station_id']: str}

        meta = pd.read_csv(io.BytesIO(fileio), encoding = "utf8", nrows = 1, header = None)
        
        towerId = meta[1][0].split("_")[1]

        self.__stationId = self.__getStationId(towerId)

        return pd.read_csv(io.BytesIO(fileio), encoding = "utf8", dtype=station_id_str, header=[1], skiprows = [2,3])

    
    def extractAndTranslateDataFromFile(self):
        df = self.__extractData(self.file)
        data = {}
        data['events'] = []
        for index, row in df.iterrows():
            event = {}
            stationId = self.__stationId
            siteInfo = [info for info in self.config['station_coords'] if (info['station_id'] == stationId)][0]
            if (siteInfo == None):
                raise ValueError("Site is not Mappable.")              
            for param, val in self.config['postDataMap'].items():
                if not val:
                    continue
                if (param == 'datetime'):
                    dt = arrow.get(row[val], "YYYY-M-D H:mm:ss")
                    dt = dt.replace(tzinfo=siteInfo['utc_offset'])
                    event[param] = int(dt.timestamp * 1e3)
                elif (param == 'site_id'):
                    event[param] = "CAF"
                elif (param == 'station_id'):
                    event[param] = self.__stationId
                elif (param == 'record_type'):
                    event[param] = "L"
                else:
                    event[param] = row[val]
            event['x_coord'] = siteInfo['long']
            event['y_coord'] = siteInfo['lat']
            event['site_id'] = siteInfo['site_id']
            data['events'].append(event)
        # only 
        if self.last30daysonly:
            prev30days = int((arrow.utcnow().shift(days=-30)).timestamp*1e3)
            data['events'] = [value for value in data['events'] if value['datetime']>=prev30days]
        return data

    def __getStationId(self, towerId):
        stationId = ""

        if towerId == "CookEast":
            stationId = "US-CF1"
        elif towerId == "CookWest":
            stationId = "US-CF2"
        elif towerId == "BoydNorth":
            stationId = "US-CF3"
        elif towerId == "BoydSouth":
            stationId = "US-CF4"
        else:
            stationId = "999"
        
        return stationId
