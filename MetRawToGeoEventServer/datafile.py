# -*- coding: utf-8 -*-
"""

abstract class
designed to provide interface to allow easy extension of data translation
from some input format to an expected output format.

Require 5 parameters as part of every output event:
    1) 'datetime' in the following formats:
        Tue Mar 12 14:25:30 PDT 2019
        03/12/2019 02:25:30 PM
        03/12/2019 14:25:30
        1552400730000
    2) 'site_id':
        unique site/location id across network
    3) 'station_id':
        unique station id within the site.
    4) 'x_coord'/'longitude'
    5) 'y_coord'/'latitude'
    
    example expected output:
    
    {
     "events": [
            {
             'datetime' : 1552400730000,
             'site_id' : 'CPER',
             'station_id' : 'station_id',
             'x_coord': -104.0000,
             'y_coord': 43.0000,
             
             <your data key/value pairs to follow>
             
            }
        ]
    }
    
@author: roger.marquez
"""

class DataFile:
    def __init__(self,
                 file: str,
                 last30only: bool,
                 config: dict):
        self.file = file
        self.last30daysonly = last30only
        self.config = config
         
    def extractAndTranslateDataFromFile(self):
        pass