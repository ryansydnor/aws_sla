from __future__ import division
import os
import re
import json
import requests
from datetime import datetime

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(THIS_DIR, 'data')

def main():
    outages = get_aws_outages()
    specific_service_outages = group_outages_by_name(outages, lambda x: x['service_name'])
    # remove parenthesis from service names (that contain regions)
    general_service_outages = group_outages_by_name(outages, lambda x: re.sub(r'\(.*?\)', '', x['service_name']).strip())
    total_days = get_total_number_of_days_since_first_data_point(outages)
    specific_service_sla = calculate_sla(specific_service_outages, total_days)
    general_service_sla = calculate_sla(general_service_outages, total_days)
    write_to_json(specific_service_sla, 'specific_service_sla.json')
    write_to_json(general_service_sla, 'general_service_sla.json')


def get_aws_outages():
    outages = []
    # populate historic data from files
    # this data was retrieved using the wayback machine
    for (directory, _, files) in os.walk(DATA_DIR):
        for file_name in files:
            abs_file_path = os.path.join(directory, file_name)
            outages.extend(get_outages_from_file(abs_file_path))

    # pull most recent data from aws
    outages.extend(get_outages_from_url('http://status.aws.amazon.com/data.json'))
    return outages

def get_outages_from_file(path):
    with open(path) as data_file:
        return consolidate_aws_outage_json(json.load(data_file))

def get_outages_from_url(url):
    return consolidate_aws_outage_json(requests.get(url).json())

def consolidate_aws_outage_json(aws_outage_json):
    result = aws_outage_json['archive']
    result.extend(aws_outage_json['current'])
    return result

def get_total_number_of_days_since_first_data_point(outages):
    # number of days missing in my data sample between 8/29/10 and 12/23/09
    #                                                  11/1/13 and 10/11/13
    earliest_date = min([datetime.fromtimestamp(float(x['date'])) for x in outages])
    days = (datetime.today() - earliest_date).days - (249 + 21)
    return days

def convert_to_dt(timestamp):
    return datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d')

def group_outages_by_name(outages, name_func):
    result = {}
    for outage in outages:
        service_name = name_func(outage)
        service_outages = result.get(service_name, [])
        # for now, only allowing one incident a day since we are calculating with the
        # granularity of one day
        outage_exists_on_same_day = [x for x in service_outages if convert_to_dt(x['date']) == convert_to_dt(outage['date'])]
        if not outage_exists_on_same_day:
            service_outages.append(outage)
            # is this necessary?
            result[service_name] = service_outages
    return result

def calculate_sla(outages_by_service, total_days):
    result = {}
    for service_name, service_outages in outages_by_service.iteritems():
        result[service_name] = (total_days - len(service_outages)) / total_days
    return result

def write_to_json(data, path):
    with open(path, 'w') as results_file:
        results_file.write(json.dumps(data, indent=4, sort_keys=True))

main()
