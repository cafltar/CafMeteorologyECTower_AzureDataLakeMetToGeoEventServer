# -*- coding: utf-8 -*-
"""
Created on Fri May 22 13:12:39 2020

@author: roger.marquez
"""
import pandas as pd
import arrow
import json
from . import datafile

class TAO5DataFile(datafile.DataFile):
    
    def __extractData(self):
        # taa5 format can be extracted following:
        # skip row 0, make row 1 the header, skip rows 2 and 3 (metadata)
        station_id_str = {self.config['postDataMap']['station_id']: str}
        return pd.read_csv(self.file, dtype=station_id_str, header=[1], skiprows = [2,3])
    
    def extractAndTranslateDataFromFile(self):
        df = self.__extractData()
#        print("Data Extracted from file...");
        data = {}
        data['events'] = []
        for index, row in df.iterrows():
            event = {}
            stationId = row[self.config['postDataMap']['station_id']]
            siteInfo = [info for info in self.config['station_coords'] if (info['station_id'] == stationId)][0]
            if (siteInfo == None):
                raise ValueError("Site is not Mappable.")              
            for param, val in self.config['postDataMap'].items():
                if not val:
                    continue
                if (param == 'datetime'):
                    
                    dt = arrow.get(row[val], "M/D/YYYY H:mm Z",)
                    dt = dt.replace(tzinfo=siteInfo['timezone'])
                    event[param] = dt.timestamp * 1e3
                else:
                    event[param] = row[val]
            event['x_coord'] = siteInfo['long']
            event['y_coord'] = siteInfo['lat']
            event['site_id'] = siteInfo['site_id']
            data['events'].append(event)
        # only 
        if self.last30daysonly:
            prev30days = int((arrow.utcnow().shift(days=-30)).timestamp*1e3)
            #print(json.dumps(data['events'][0], indent=4));
            data['events'] = [value for value in data['events'] if value['datetime']>=prev30days]
#        print(json.dumps(data, indent=4));
#        print("Records converted to expected format...");
        return data

