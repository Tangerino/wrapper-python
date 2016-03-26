#! /usr/bin/env python
import pyowm
from wimd import wimd
import sys
import datetime
from datetime import timedelta
import requests
from xml.etree import ElementTree

devkey = 'a070c96e-b973-11e5-8b2d-04017fd5d401'
weatherKey = '4d0c7fc995ff9152b5957014e5035710'
owm        = pyowm.OWM(weatherKey)

cities = [
{"_id":6454071,"name":"Grenoble","country":"FR","coord":{"lon":5.7266,"lat":45.187199}},
{"_id":3448439,"name":"Sao Paulo","country":"BR","coord":{"lon":-46.636108,"lat":-23.547501}},
{"_id":3451190,"name":"Rio de Janeiro","country":"BR","coord":{"lon":-43.2075,"lat":-22.902781}},
{"_id":5880054,"name":"Barrow","country":"US","coord":{"lon":-156.788727,"lat":71.290581}},
{"_id":3467865,"name":"Campinas","country":"BR","coord":{"lon":-47.060829,"lat":-22.90556}},
{"_id":292223,"name":"Dubai","country":"AE","coord":{"lon":55.304722,"lat":25.258169}},
{"_id":2013159,"name":"Yakutsk","country":"RU","coord":{"lon":129.733063,"lat":62.03389}},
{"_id":5178813,"name":"Bagdad","country":"US","coord":{"lon":-80.024223,"lat":41.952}},
{"_id":2449067,"name":"Timbuktu","country":"ML","coord":{"lon":-3.00742,"lat":16.773479}},
{"_id":2064144,"name":"Oodnadatta","country":"AU","coord":{"lon":135.466675,"lat":-27.549999}},
{"_id":2992908,"name":"Montbonnot-Saint-Martin","country":"FR","coord":{"lon":5.80353,"lat":45.222359}},
{"_id":3006131,"name":"La Tronche","country":"FR","coord":{"lon":5.73645,"lat":45.204288}},
{"_id":2968815,"name":"Paris","country":"FR","coord":{"lon":2.3486,"lat":48.853401}},
{"_id":5128638,"name":"New York","country":"US","coord":{"lon":-75.499901,"lat":43.000351}}
]

def telvent(lat, lon, start_date, end_date):
    socket = requests.session()
    url = 'http://weather.dtn.com/basic/rest-3.4/obsfcst.wsgi?dataType=HourlyObservation&dataTypeMode=0001&Temperature=C&startDate={0}&endDate={1}&RelativeHumidity=1&latitude={2}&longitude={3}'
    page = url.format(start_date, end_date, lat, lon)
    try:
        r = socket.get(page, auth=('sefio2287761', 's3l0E39XjJ0K85z1'))
        return r.status_code == 200, r.content
    except:
        pass
    return False, {}





def add_sensor(id, name, timestamp, value, unit, unitname, description=''):
    rc, resp = w.sensor_new(devkey,id,name,unit,unitname,description,"")
    if rc:
        series = []
        values = []
        value = (timestamp,value)
        values.append(value)
        serie = {"id": id, "values": values}
        series.append(serie)
        rc = w.sensor_add_data(devkey,series)
        if rc == False:
            rc = -2
    else:
        print(resp)
        rc = -1
    return rc

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    p = '%Y-%m-%dT%H:%M:%S'
    yesterday = datetime.datetime.utcnow() - timedelta(hours=24)
    today     = datetime.datetime.utcnow() - timedelta(minutes=5)
    rc, xml = telvent('44.880277', '-93.216666', yesterday.strftime(p), today.strftime(p))
    if rc == True:
        root = ElementTree.fromstring(xml)
        lr = root.getchildren()
        l = lr[0].getchildren()
        time_stamp = ''
        temperature = 0
        humidity = 0
        for x in l:
            d = x.getchildren()
            for d2 in d:
                print(d2.tag, d2.text)
                if d2.tag == 'validDateTime':
                    time_stamp = d2.text
                elif d2.tag == 'temperature':
                    temperature = 0
                elif d2.tag == 'relativeHumidity':
                    humidity = 0



    w = wimd()
    for p in range(len(cities)):
        city = cities[p]
        print('Collecting weather data from ' + city['name'])
        try:
            observation = owm.weather_at_id(city['_id'])
            weather = observation.get_weather()
            t = weather.get_temperature('celsius')
            u = weather.get_humidity()
            today = datetime.datetime.utcnow().strftime(p)
            print ('Temperature now in ' + city['name'] + ' is ' + str(t['temp']) + ' with humidity of ' + str(u) + ' %')
            cid = city['name'] + '_' + city['country']
            name = city['name'] + ' - ' + city['country']
            rc = add_sensor(cid + '_C', name, today, t['temp'], 'C', 'Temperature', "")
            if rc > 0:
                rc = add_sensor(cid + '_RH', name, today, u, '%', 'Relative Humidity', "")
            if rc <= 0:
                print("Error creating sensor")
        except:
            print('Error reading data from ' + city['name'])
            print(sys.exc_info()[0])
