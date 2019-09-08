"""
This lambda function is fired when one or more  XML files are uploaded to
the given S3 bucket. It will perform,
1. Download each file to tmp folder
2. Parse each XML file, retrieve the required fields and write to a JSON file
3. Upload the JSON file to the processed folder
"""

import boto3
import os
import uuid
import json
from urllib.parse import unquote_plus
import xml.etree.ElementTree as ET
from collections import OrderedDict

s3_client = boto3.client('s3')


def get_keys_map(xml_fields, output_fields):
    """Return a dict which xml field name is key, json field name is value"""
    keys_map = {k: v for k, v in zip(xml_fields, output_fields)}
    return keys_map


def parse_xml(download_path, upload_path):
    """Extract the required fields from XML, change the field names if required

    Params:
    =======
        download_path:  the file path containing the XML file
        upload_path:    the file path for the processed JSON file

    Returns:
    ========
        None
    """
    # fields required to extract from XML
    xml_fields = [
        'League', 'Channel', 'Start_Date', 'Start_Time', 'Programme_Title',
        'Live__L_', 'Episode_Title', 'Episode_Number', 'Synopsis',
        'Opta_Home_Team_ID', 'Opta_Away_Team_ID', 'Opta_Match_ID', 'Duration',
        'Match', 'House_No']
    # fields for JSON output
    json_fields = [
        'SourceFile', 'League', 'Channel', 'Start_Date_SGT', 'Start_Time_SGT',
        'Programme_Title', 'isLive', 'Episode_Title', 'Episode_Number',
        'Synopsis', 'Opta_Home_Team_ID', 'Opta_Away_Team_ID', 'Opta_Match_ID',
        'Duration', 'Match', 'House_No']
    keys_map = get_keys_map(xml_fields, json_fields[1:])

    # read the XML file
    tree = ET.parse(download_path)
    root = tree.getroot()

    with open(upload_path, 'w') as f:
        # loop through the XML tree to retrieve the records
        for r in root.iter():
            if not r.tag.endswith('Details'):
                continue
            atts = r.attrib
            row = OrderedDict()
            # because there is no SourceFile field in the XML file,
            # assign it to the basename source_file
            row['SourceFile'] = os.path.splitext(os.path.basename(download_path))[0]
            row.update({
                new_k: atts.get(old_k, '') for old_k, new_k in keys_map.items()
            })
            f.write(json.dumps(row) + '\n')


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        upload_path = '/tmp/processed-{}'.format(key)
        s3_client.download_file(bucket, key, download_path)
        parse_xml(download_path, upload_path)
        s3_client.upload_file(upload_path, '{}processed'.format(bucket), key)


# Below code is for local testing
# if __name__ == '__main__':
#     download_path = './epg_2019_08_12_190008.xml'
#     upload_path = './output/test3_output.json'
#     parse_xml(download_path, upload_path)
