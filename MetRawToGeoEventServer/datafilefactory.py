# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 10:17:24 2020

@author: roger.marquez
"""

from . import datafile
from . import tao5datafile
from . import naldatafile
from . import azuredatalaketoa5datafile

class DataFileFactory:
    @staticmethod
    def GenerateDataFileByType(dfType: str, file: str, limitData30days: bool, cfg: dict) -> datafile.DataFile:
        ttype = dfType.lower()
        if ttype == 'tao5':
            return tao5datafile.TAO5DataFile(file, limitData30days, cfg)
        elif ttype == 'nal':
            return naldatafile.NALDataFile(file, limitData30days, cfg)
        elif ttype == 'caf':
            return azuredatalaketoa5datafile.AzureDataLakeTOA5DataFile(file, limitData30days, cfg)
        else:
            raise ValueError(dfType)             


        
