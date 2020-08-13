from datetime import datetime, timedelta
from googleapiclient.discovery import build
from mapbox import Geocoder

import boto3
import json
import logging
import os
import requests
import sys

SHEET_RANGE = "'Main'"
GEOJSON_URL = "https://dailynexus.s3-us-west-1.amazonaws.com/crime-data.json"

FIELDS = {
    'location': 0,
    'date': 1,
    'age': 3,
    'school-affiliation': 4,
    'city-residence': 5,
    'time': 6,
    'crime': 11
}

PROXIMITY = [-119.861, 34.413]

log = logging.getLogger(__name__)

def load_sheet():
    service = build('sheets', 'v4', developerKey=os.environ["GOOGLE_API_KEY"], cache_discovery=False)

    # Call Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=os.environ["SPREADSHEET_ID"], range=SHEET_RANGE).execute()
    values = result.get('values', [])

    rows = {}
    empty = {}

    for field in FIELDS:
        empty[field] = ''

    for row in values[1:]:
        props = {}
        props.update(empty)

        for field in FIELDS:
            idx = FIELDS[field]
            if idx < len(row):
                props[field] = row[idx].strip()

        props['month'] = props['date'][0:7]

        if row[FIELDS['location']]:
            row_key = row[FIELDS['location']].strip() + row[FIELDS['date']].strip() + row[FIELDS['time']].strip()
            rows[row_key] = {'properties': props}

    return rows

def load_geojson():
    resp = requests.get(GEOJSON_URL)
    features = {}
    for feature in resp.json()['features']:
        key = feature['properties']['location'] + feature['properties']['date'] + feature['properties']['time']
        features[key] = feature
    
    return features

def get_geodata(sheet, keys, countries):
    # Should be called with keys newly added to the spreadsheet
    geocoder = Geocoder()

    for key in keys:
        location = sheet[key]['properties']['location']
        response = geocoder.forward(location, limit=1, country=countries, lon=PROXIMITY[0], lat=PROXIMITY[1]).geojson()
        if 'features' in response and response['features']:
            feature = response['features'][0]
            log.info('Geocoding %s\n\t%s', location, feature)
            
            if feature['relevance'] < 0.75:
                log.warning('Error geocoding %s', location)
                continue

            sheet[key]['geometry'] = feature['geometry']
            place_name = feature.get('place_name')
            if place_name:
                sheet[key]['properties']['placeName'] = place_name
        else:
            if key in sheet:
                del sheet[key]
            log.warning('Error geocoding %s', key)

def merge_data(sheet, dataset):
    for key in sheet:
        row = sheet[key]
        if key in dataset and row['properties'] == dataset[key]['properties']:
            log.info('%s unchanged', key)
            continue
        log.info('Updating %s', key)
        if key in dataset:
            dataset[key]['properties'].update(row['properties'])
        else:
            dataset[key] = row
        dataset[key]['Type'] = 'Feature'
        if not dataset[key].get('geometry', None):
            log.info('%s missing geometry; deleting', key)
            del dataset[key]

    orphans = []
    for key in dataset:
        if key in sheet:
            log.info('%s in both dataset and sheet', key)
            continue
        orphans.append(key)
    log.info('%s orphans', len(orphans))
    for key in orphans:
        del dataset[key]

    return dataset

def upload(dataset, dry_run=False):
    data = {
        'type': 'FeatureCollection',
        'features': [dataset[key] for key in dataset]
    }

    if dry_run:
        print(data)
    else:
        print("Uploading data")
        s3 = boto3.resource('s3')
        response = s3.Object('dailynexus', 'crime-data.json').put(
                Body=json.dumps(data, indent=2),
                ContentType='application/json',
                ACL='public-read',
                Expires=(datetime.now() + timedelta(hours=48))
        )
        log.info(response)

def lambda_handler(event=None, context=None, dry_run=False):
    sheet = load_sheet()
    data = load_geojson()

    keys = sheet.keys() - data.keys()
    if keys:
        get_geodata(sheet, keys, ['us', 'ca'])

    merge_data(sheet, data)
    upload(data, dry_run)

def main():
    dry_run_cmd = sys.argv[1].lower() == 'true'
    lambda_handler(None, None, dry_run_cmd)

if __name__ == '__main__':
    main()
