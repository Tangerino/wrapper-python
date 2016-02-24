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
from xml.sax.saxutils import escape
import codecs
import re

import sys
import time

from wimd import wimd


APIKEY = 'b59cc3e9-dafd-11e5-8bcd-04017fd5d401'


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


def WIMDLoadTask(doc, feeds, output_folder, sensor_id):
    w = wimd()
    series = []
    total_data_points = 0
    for s in sensor_id:
        print(sensor_id[s])
        id = sensor_id[s]['id']
        n = sensor_id[s]['name']
        un = sensor_id[s]['fn']
        u = sensor_id[s]['unit']
        rc, j = w.sensor_new(APIKEY, id, n, u, un)
        if rc:
            # print(id, n, un, u)
            values = []
            for feed in feeds:
                if feed['id'] == id:
                    ts = feed['ts'].strftime("%Y-%m-%dT%H:%M:%S")
                    v = float(feed['value'])
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
                    print(
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(total_data_points) + ' so far', n, u)
                else:
                    print(rc, j)
        else:
            print('Error creating sensor ' + sensor_id[s]['id'], j)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(total_data_points) + ' sent')
    return total_data_points


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
            if row_number == 0:  # header
                header1 = (row)
            elif row_number == 2:  # empty]
                pass
            elif row_number == 3:
                pass
            elif row_number == 4:  # topic ids
                topicids = (row)
            elif row_number == 5:  # empty
                pass
            elif row_number == 6:  # data header
                dataHeader = (row)
                doc['desc'] = str(header2) + '\r\n' + str(topicids) + '\r\n' + str(dataHeader)
            elif row_number == 1:  # file header
                header2 = row
                doc['gatewayname'] = escape(row[0]).decode('utf-8')
                doc['serial'] = escape(row[1]).decode('utf-8')
                doc['devicename'] = escape(row[4]).decode('utf-8')
                doc['deviceid'] = str(row[5]).decode('utf-8')
                doc['devicetype'] = str(row[6]).decode('utf-8')
                doc['mac'] = header2[3].decode('utf-8')
            else:
                if len(row) > 0 and row[0] == '0':  # status 0 is a good data sample
                    try:
                        ts = datetime.datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
                        tz = int(row[1])
                        ts -= datetime.timedelta(minutes=tz)
                        id = doc['devicename'] + "_" + doc['deviceid'] + "_"
                        col_number = 0
                        feed = []
                        for col in row:
                            if col_number >= 3:
                                topic = topicids[col_number]
                                feedName = dataHeader[col_number].decode('utf-8')
                                Unit = getUnit(feedName)
                                fk = doc['mac'] + '.' + re.sub('[^0-9a-zA-Z]', '_', feedName)
                                fid = doc['mac'] + '.' + doc['devicename'] + '.' + str(doc['deviceid']) + '.' + Unit
                                try:
                                    feed = {'id': fid, 'fn': get_uom_name(feedName), 'ts': ts, 'value': col,
                                            "unit": Unit, 'fk': fk}
                                    feeds.append(feed)
                                    sensor_id[fid] = {'id': fid, 'fn': get_uom_name(feedName), "unit": Unit,
                                                      'name': doc['devicename']}
                                    # print(feed)
                                except:
                                    pass
                            col_number += 1
                    except:
                        pass
            row_number += 1
        tdp = WIMDLoadTask(doc, feeds, output_folder, sensor_id)
        i_file.close()
        return tdp
    return -1


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
        input_folder = '/Users/carlos/Desktop/etl/csv/'
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
