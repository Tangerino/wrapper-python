#! /usr/bin/env python

'''
This ETL parses an EGX300/ComX-200 CSV file format (Schneider Electric Gateway) and
exports it under a WIMD.IO device
Please request an APIKEY for your device at http://wimd.io

This is a 'under development ETL'

Carlos Tangerino
'''


import os
import csv
import getopt
import json
import datetime
import codecs
import re

import sys
import time

from wimd import wimd


APIKEY = '39937ab8-ef6c-11e5-8bdb-04017fd5d401'


def getUnit(feedName):
    # print (feedName)
    start = feedName.find("(")
    if start >= 0:
        end = feedName.find(")", start + 1)
        if end > (start + 1):
            unit = feedName[start + 1:end]
            return unit
    return ""


def get_uom_name(feedName):
    start = feedName.find(" (")
    if start >= 0:
        return feedName[0:start]
    return feedName


def usage():
    print('Usage: egx2wimd.py -i <input files path> -o <output files path>')


def WIMDLoadTask(sensor_id, feeds, output_folder):
    w = wimd()
    series = []
    total_data_points = 0
    for s in sensor_id:
        print(sensor_id[s])
        id = sensor_id[s]
        n = sensor_id[s]
        un = 'Current'
        u = 'A'
        rc, j = w.sensor_new(APIKEY, id, n, u, un)
        if rc:
            # print(id, n, un, u)
            values = []
            for feed in feeds:
                if feed['id'] == id:
                    ts = feed['ts']
                    v = float(feed['v'])
                    d = (ts, v)
                    values.append(d)
                    if len(values) >= 5000:
                        serie = {'id': id, 'values': values}
                        series.append(serie)
                        x = json.dumps(series)
                        # print(x)
                        rc, j = w.sensor_add_data(APIKEY, series)
                        if rc == True:
                            total_data_points += len(values)
                            print(
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                str(total_data_points) + ' so far',
                                n, u)
                        else:
                            print(rc, j)
                        series = []
                        values = []
            if len(values) > 0:
                serie = {'id': id, 'values': values}
                series.append(serie)
                rc, j = w.sensor_add_data(APIKEY, series)
                if rc == True:
                    total_data_points += len(values)
                    #print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(total_data_points) + ' so far', n, u)
                else:
                    print(rc, j)
        else:
            print('Error creating sensor ' + sensor_id[s]['id'], j)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(total_data_points) + ' sent')
    return total_data_points

# SiteId	DeviceId	MeterId	Horodate	        NumericValue
# 54	    431	        3	    2016-02-21T00:15:00	5
# 54	    433	        3	    2016-02-21T00:15:00	0
def process_file(fname, output_folder):
    reader = []
    doc = {}
    print("Parsing " + fname)
    try:
        i_file = codecs.open(fname, "rU")
        dialect = csv.Sniffer().sniff(i_file.read(32))
        i_file.seek(0)
        reader = csv.reader(i_file, dialect)
    except:
        print("Can't open " + fname)
        return -1
    finally:
        feeds = []
        sensor_id = {}
        row_number = 0
        for row in reader:
            if row_number > 0:  # header
                try:
                    sensor = 'FI_SITE_' + row[0] + '_DEVICE_' + row[1] + '_METERID_' + row[2]
                    sensor_id[sensor] = sensor
                    feed = {'id': sensor,'ts':row[3],'v':float(row[4])}
                    feeds.append(feed)
                except:
                    pass
            row_number += 1
        rc = WIMDLoadTask(sensor_id, feeds, output_folder)
        return rc

def process_files(input_folder, output_folder, force):
    total_data_points = 0
    names = os.listdir(input_folder)
    try:
        os.makedirs(output_folder)
    except:
        pass
    for name in names:
        src_name = os.path.join(input_folder, name)
        dst_name = os.path.join(output_folder, name)
        try:
            if os.path.islink(src_name):
                pass
            if os.path.isdir(src_name):
                # will we support go deeper in the file hierarchy?
                # process_files(gateways, src_name, output_folder, force)
                pass
            if (name.lower().find(".csv") > 0):
                tdp = process_file(src_name, output_folder)
                if tdp > 0:
                    total_data_points += tdp
                    os.rename(src_name, dst_name)
        except (IOError, os.error) as why:
            print((src_name, dst_name, str(why)))
    return total_data_points


def main(argv):
    start_time = time.time()
    input_folder = ''
    output_folder = ''
    force = 1
    try:
        opts, args = getopt.getopt(argv, "fhi:o:", ["ifiles=", "ofiles=", "force"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        if opt == '-f':
            force = 1
        elif opt in ("-i", "--ifiles"):
            input_folder = arg
        elif opt in ("-o", "--ofiles"):
            output_folder = arg
    if (input_folder == ''):
        input_folder = '/Users/carlos/Desktop/Unbalance/'
    if (output_folder == ''):
        output_folder = input_folder + '/done'
        try:
            os.mkdir(output_folder)
        except:
            pass
    if (input_folder == '' or output_folder == ''):
        usage()
        sys.exit(3)
    print('Input folder is "' + input_folder + '"')
    print('Output folder is "' + output_folder + '"')
    tdp = process_files(input_folder, output_folder, force)
    end_time = time.time()
    print('Total of {0} data points set in {1} seconds'.format(tdp, end_time - start_time))


if __name__ == "__main__":
    main(sys.argv[1:])
