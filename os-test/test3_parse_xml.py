import json
import xml.etree.ElementTree as ET
from collections import OrderedDict


def get_keys_map(xml_fields, output_fields):
    """Return a dict which xml field name is key, json field name is value"""
    keys_map = {k: v for k, v in zip(xml_fields, output_fields)}
    return keys_map


def parse_xml(xml_string, source_file):
    """Extract the required fields from XML, change the field names if required

    Params:
    =======
        xml_string:   a string contains the XML content
        source_file:  the name of the source file

    Returns:
    ========
        a list of dicts for the records
    """

    # fields required to extract from XML
    xml_fields = [
        'League', 'Channel', 'Start_Date', 'Start_Time', 'Programme_Title',
        'Live__L_', 'Episode_Title', 'Episode_Number', 'Synopsis',
        'Opta_Home_Team_ID', 'Opta_Away_Team_ID', 'Opta_Match_ID', 'Duration',
        'Match', 'House_No']
    # fields for JSON output
    output_fields = [
        'SourceFile', 'League', 'Channel', 'Start_Date_SGT', 'Start_Time_SGT',
        'Programme_Title', 'isLive', 'Episode_Title', 'Episode_Number',
        'Synopsis', 'Opta_Home_Team_ID', 'Opta_Away_Team_ID', 'Opta_Match_ID',
        'Duration', 'Match', 'House_No']
    keys_map = get_keys_map(xml_fields, output_fields[1:])
    results = []
    root = ET.fromstring(xml_string)

    # loop through the XML tree to retrieve the records
    for r in root.iter():
        if r.tag.endswith('Details'):
            atts = r.attrib
            row = OrderedDict()
            # because there is no SourceFile field in the XML file,
            # assign it to the augument source_file
            row['SourceFile'] = source_file
            row.update({
                new_k: atts.get(old_k, '') for old_k, new_k in keys_map.items()
            })
            results.append(row)
    return results


if __name__ == '__main__':
    file_path = './test3_input.xml'
    file_name = 'epg_2019_08_12_190008'
    xml_string = open(file_path, 'r').read()
    res = parse_xml(xml_string, file_name)
    output_path = './output/test3_output.json'
    with open(output_path, 'w') as f:
        for r in res:
            f.write(json.dumps(r) + '\n')
