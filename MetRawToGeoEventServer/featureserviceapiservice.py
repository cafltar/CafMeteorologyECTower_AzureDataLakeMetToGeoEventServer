# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 13:16:43 2020

@author: roger.marquez
"""
import requests;
import arrow;

hostedRoot = "https://pdient.azurecloudgov.us/server/rest/services/Hosted/";
serviceTokenUrl = "https://pdient.azurecloudgov.us/portal/sharing/rest/generateToken"

class FeatureServiceAPIService:
    @staticmethod
    def findIDSLast30Days(featureSvcInfo: dict, stations: dict)-> dict:
        serviceTokenParams = {
        'username': featureSvcInfo['username'],
        'password': featureSvcInfo['password'],
        'client' : 'referer',
        'referer': 'https://pdient.azurecloudgov.us/server',
        'f':'json'
        };
        svcTokenRes = requests.post(serviceTokenUrl, 
                            headers = {'content-type': 'application/x-www-form-urlencoded'},
                            data=serviceTokenParams);
        if (svcTokenRes.status_code == 200):
            svcToken = svcTokenRes.json()
            if svcToken is not None:
                searchPairs = [[val['station_id'], val['site_id']] for val in stations];
                results = [];
                url = "{}/query".format(featureSvcInfo['featureSvcREST']);
                currTime = arrow.utcnow();
                prev30 = currTime.shift(days=-30)
                params={
                        'outFields': 'record_id,datetime,site_id,station_id',
                        'orderByFields': 'datetime',
                        'token': svcToken['token'],
                        'f': 'json'
                        }
                for station, site in searchPairs:
                    statid = '{0}{1}'.format(site,station);
                    query = 'datetime >= timestamp \'{0}\''.format(prev30.format('YYYY-MM-DD HH:mm:ss'));
                    query += ' and datetime < timestamp \'{0}\''.format(currTime.format('YYYY-MM-DD HH:mm:ss'));
                    query += ' and site_id=\'{0}\''.format(site);
                    query += ' and station_id=\'{0}\''.format(station);
                    params['where'] = query;
                    data = requests.get(url, params=params);
                    if (data.status_code == 200):
                        jsonRes = data.json();
                        results.extend([feature['attributes']['record_id'] for feature in jsonRes['features']]); # collect IDs
                return results
        return None
            


            
