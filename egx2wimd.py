#! /usr/bin/env python

'''
This ETL parses an EGX300-CSV (Schneider Electric Gateway) and
exports it under a WIMD.IO device
Please request an APIKEY for your device at http://wimd.io

This is a 'under development ETL'

Carlos Tangerino
'''

import os
import csv
import sys
import getopt
import json
import time
import datetime
from xml.sax.saxutils import escape
import codecs
import re

from wimd import wimd


APIKEY = 'YOUR DEVICE KEY HERE'


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
    print ('Usage: egx.py -i <input files path> -o <output files path>')


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
                        #print(x)
                        rc, j = w.sensor_add_data(APIKEY, series)
                        if rc == True:
                            total_data_points += len(values)
                            print(
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(total_data_points) + ' so far',
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
        i_file = codecs.open(fname, "rb")
        dialect = csv.Sniffer().sniff(i_file.read(32))
        i_file.seek(0)
        reader = csv.reader(i_file, dialect)
    except:
        print "Can't open " + fname
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
                        tz = row[1]
                        ts -= datetime.timedelta(minutes=tz)
                        id = doc['devicename'] + "_" + doc['deviceid'] + "_"
                        colnumber = 0
                        feed = []
                        for col in row:
                            if colnumber >= 3:
                                topic = topicids[colnumber]
                                feedName = dataHeader[colnumber].decode('utf-8')
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
                            colnumber += 1
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
        srcname = os.path.join(input_folder, name)
        dstname = os.path.join(output_folder, name)
        try:
            if os.path.islink(srcname):
                pass
            if os.path.isdir(srcname):
                # will we support go deeper in the file hierarchy?
                # process_files(gateways, srcname, output_folder, force)
                pass
            if (name.lower().find(".csv") > 0):
                tdp = process_file(srcname, output_folder)
                if tdp > 0:
                    total_data_points += tdp
        except (IOError, os.error) as why:
            print((srcname, dstname, str(why)))
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
        input_folder = '/Users/carlos/Desktop/archive/wimd/Carlos'
    if (output_folder == ''):
        output_folder = '/Users/carlos/Desktop/archive/wimd/Carlos'
    if (input_folder == '' or output_folder == ''):
        usage()
        sys.exit(3)
    print ('Input folder is "' + input_folder + '"')
    print ('Output folder is "' + output_folder + '"')
    tdp = process_files(input_folder, output_folder, force)
    end_time = time.time()
    print('Total of {0} data points set in {1} seconds'.format(tdp, end_time - start_time))


if __name__ == "__main__":
    main(sys.argv[1:])