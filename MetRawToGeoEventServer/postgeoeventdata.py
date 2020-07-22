# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 13:24:47 2020

@author: roger.marquez
"""
import requests
import json
import argparse
import glob, os
from . import datafilefactory
from . import featureserviceapiservice

class PostGeoEventData:
        def __init__(self, cfgFile: str):
            self.cfgFile = cfgFile
            self.cfg = self.__readConfig(cfgFile)
            
        def __readConfig(self, cfgFile: str):
            with open(cfgFile) as f:
                return json.load(f)
            
        #This method:
        # 1) gets top two recently edited CSVs from the input path.
        # 2) gets all ID's from last 30 days.
        # 3) processes the top two CSVs and extracts last 30 days worth of data.
        # 4) removes all records that are already in the server.
        # 5) if there are any remaining values: post these values to the endpoint.
        def processGeoEventPost(self, inputPath: str):

            inputConfig = self.cfg['inputType']
            fileType = inputConfig['fileType']
            svcCfg = self.cfg['featureSvcInfo']
            files = []
            events = []
            
            # Set the following for FALSE to run for production.
            # False triggers pushing ALL records to the endpoint.
            # must still update config for production endpoint.
            testing = False
            
            if os.path.isdir(inputPath):
                #If path: find top 2 previously modified files with extension
                files = self.__getTopTwoMostRecentFiles(inputPath, "*" + inputConfig['fileExt'])   
            else:
                # else file was provided so process that file.
                files = [inputPath]
            
            #only need to hit api once...
            if not testing:
                IDs = featureserviceapiservice.FeatureServiceAPIService.findIDSLast30Days(svcCfg,  self.cfg['station_coords'])
            else:
                IDs = []
            #can test with empty ID field so all records are transmitted.
            #IDs=[]
            for fname in files:
                #generate object to process the input file(s)
                datafile = datafilefactory.DataFileFactory.GenerateDataFileByType(
                       dfType=fileType,
                       file=fname,
                       # Set the next variable to False to trigger attempting to push all processed data.
                       limitData30days=True,
                       cfg=self.cfg
                       )
                
                if datafile == None:
                    raise ValueError("Unsupported filetype: {}".format(fileType))
                
                # Extract/translate the data.
                data = datafile.extractAndTranslateDataFromFile()
                
                # Remove records that already exist in the feature service.
                events = events + [value for value in data['events'] if '{0}{1}{2}'
                        .format(value['datetime'],value['site_id'],value['station_id']) not in IDs]
            
            # All file(s) are processed, if there is data left
            # attempt to post it to the REST service.
            print("number being posted: ", len(events))
            if len(events) > 0:
                # Testing: this only posts 4 records...
                if testing:
                    self.__postGeoEventData({'events': events[0:4]})
                else:
                    #use the folllowing two lines to run "production"
                     #Chunks data into 1000 records at a time.
                    for i in range(0, len(events), 1000):
                        self.__postGeoEventData({'events': events[i:i + 1000]})

        def __postGeoEventData(self, payload):
            url = self.cfg['agcrosREST']
            postStatus = requests.post(url, json=payload)
            #Not sure if we care about post status at this time.
            #Unless endpoint/records are out of date by > 30 days, 
            # we are able to pick back up within that time frame
            # if records > 30 days, something else big is going on.
            return postStatus
            
        def __globFilesInPath(self, path, extension):
            search = os.path.join(path, extension)
            for name in glob.glob(search):
                yield name
        
        def __getTopTwoMostRecentFiles(self, fullPathToDir: str, ext: str):
            try:
                return sorted(self.__globFilesInPath(fullPathToDir,ext), key=os.path.getmtime, reverse=True)[:2]
            except:
                print ("getTopTwoMostRecentFiles: error")
   
def main():
    parser = argparse.ArgumentParser(
            description='Post CSV Data to AgCROS GeoEvent Server.'
            )
    parser.add_argument(
            'path', 
            metavar='Path to input file or file(s)', 
            type=str,
            help='a path to an input file containing data to post to the server.')
    parser.add_argument(
            'cfg',
            metavar='config file path',
            type=str,
            help='a configuration file must be provided.'
            )

    args = parser.parse_args()
    geoEventPost = PostGeoEventData(args.cfg)
    geoEventPost.processGeoEventPost(args.path)    

if __name__ == '__main__':
    main()
    