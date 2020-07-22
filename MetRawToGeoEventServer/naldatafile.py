# -*- coding: utf-8 -*-
"""
Created on Fri May 22 13:13:53 2020

@author: roger.marquez
"""
import pandas as pd;
import arrow
import json;
from . import datafile

class NALDataFile(datafile.DataFile):
    
    def __extractData(self):
        station_id_str = {self.config['postDataMap']['station_id']: str}
        return pd.read_csv(self.file, dtype=station_id_str, keep_default_na=False)
        
    def extractAndTranslateDataFromFile(self):
        df = self.__extractData()
        #print (df.head())
        data = {}
        data['events'] = []
        for index, row in df.iterrows():
            event = {}
            for param, val in self.config['postDataMap'].items():
                
                if (param == 'datetime'):
                    event[param] = int(arrow.get(row[val]).timestamp * 1e3)
                else:
                    event[param] = row[val]
            siteInfo = [info for info in self.config['station_coords'] if (info['site_id'] == event['site_id'] and info['station_id'] == event['station_id'])][0]
            #print (siteInfo)
            # will throw error if the siteid + stationid from the data does not match the config stations
            event['x_coord'] = siteInfo['long']
            event['y_coord'] = siteInfo['lat']
            data['events'].append(event)
        if self.last30daysonly:
            prev30days = int((arrow.utcnow().shift(days=-30)).timestamp*1e3)
            #print(json.dumps(data['events'][0], indent=4));
            data['events'] = [value for value in data['events'] if value['datetime']>=prev30days]
        #print(json.dumps(data['events'][0], indent=4));
        return data