# Readme

## Purpose

Send raw met data from four EC Towers to USDA PDI's ESRI GeoEvent Server

See live data here: https://pdient.azurecloudgov.us/portal/apps/webappviewer/index.html?id=2746b9a0605f4aa9b03e7a3610e34986

## Technical overview

This code requires an Azure Function App to be setup to publish to. It also requires an Event Grid subscription to trigger the function on "Rename" events in the cafltardatalake storage account. Other Event Grid sub filters: Subject begins with: "/blobServices/default/containers/raw/blobs/CafMeteorologyECTower", subject ends with: ".dat", "data.destinationUrl" string contains "\_Met\_".

A config.json file must be placed in the MetRawToGeoEventServer directory with appropriate security keys / secrets. See config-TEMPLATE.json for structure.

## Acknowledgements

postgeoeventdata code was modified from code written by Roger Marquez. Original code located in private repo: https://github.com/USDA-ARS-LTAR/agcros-geoevent-python_push_script.

## License

As a work of the United States government, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the CC0 1.0 Universal public domain dedication.
