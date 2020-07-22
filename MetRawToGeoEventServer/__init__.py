import json
import logging
import os

import azure.functions as func
import pathlib

from . import postgeoeventdata


def main(event: func.EventGridEvent):
    logging.info('version: %s', 'v0.1.0')
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    
    logging.info('Python EventGrid trigger processed an event: %s', result)

    cfgPath = pathlib.Path(__file__).parent / "config.json"    
    geoEventPost = postgeoeventdata.PostGeoEventData(cfgPath)
    geoEventPost.processGeoEventPost(event.get_json()["destinationUrl"])
   
