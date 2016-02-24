'''
This is the WIMD.IO API wrapper
'''
import datetime
from datetime import timedelta

import requests

class wimd(object):
    BASE_URL = "http://wimd.io/v2/"

    def __init__(self):
        requests.packages.urllib3.disable_warnings()
        self.__socket = requests.session()
        self.__apikey = ''
        self.__apiheader = {'apikey': ''}
        self.__isadmin = False
        self.__cancreate = False
        self.__canread = False
        self.__canupdate = False
        self.__candelete = False

    def url(self):
        return self.BASE_URL

    def apikey(self):
        return self.__apikey

    def is_admin(self):
        return self.__isadmin

    def can_create(self):
        return self.__cancreate

    def can_read(self):
        return self.__canread

    def can_update(self):
        return self.__canupdate

    def can_delete(self):
        return self.__candelete

    def login(self, email, password):
        rc = False
        payload = {'email': email, 'password': password}
        try:
            r = self.__socket.post(self.BASE_URL + 'account/login', json=payload, verify=False)
            if r.status_code == 200:
                resp = r.json()
                self.__apikey = resp['apikey']
                self.__apiheader = {'apikey': self.__apikey}
                self.__cancreate = resp['permissions']['create']
                self.__canread = resp['permissions']['read']
                self.__canupdate = resp['permissions']['update']
                self.__candelete = resp['permissions']['delete']
                self.__isadmin = resp['permissions']['admin']
                rc = True
        except:
            print("Login error ")
        return rc

    def logout(self):
        rc = False
        try:
            r = self.__socket.post(self.BASE_URL + 'account/logout', headers=self.__apiheader, verify=False)
            rc = r.status_code == 200
        except:
            pass
        return rc

    def places(self):
        try:
            r = self.__socket.get(self.BASE_URL + 'places', headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def place(self, id):
        try:
            r = self.__socket.get(self.BASE_URL + 'place/' + id, headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def things(self, placeid):
        try:
            r = self.__socket.get(self.BASE_URL + 'place/' + placeid + '/things', headers=self.__apiheader,
                                  verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def sensors(self, thing_id):
        try:
            r = self.__socket.get(self.BASE_URL + 'thing/' + thing_id + '/sensors', headers=self.__apiheader,
                                  verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def sensor(self, sensorid):
        try:
            r = self.__socket.get(self.BASE_URL + 'sensor/' + sensorid, headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def sensor_new(self, devkey, remoteid, name, unit='', unitname='', description='', meta=''):
        payload = {'remoteid': remoteid, 'name': name, 'unit': unit, 'description': description, 'meta': meta,
                   'unitname': unitname}
        try:
            h = {'devkey': devkey}
            r = self.__socket.post(self.BASE_URL + 'sensor', json=payload, headers=h, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            pass
        return False, {}

    def sensor_add_data(self, devkey, data):
        try:
            h = {'devkey': devkey}
            r = self.__socket.post(self.BASE_URL + 'sensor/data', json=data, headers=h, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
            else:
                return False, r.json()
        except:
            pass
        return False, {}

    def sensor_last_value(self, sensorid):
        try:
            p = '%Y-%m-%dT%H:%M:%S'
            yesterday = datetime.datetime.now() - timedelta(hours=24)
            today = datetime.datetime.now() + timedelta(hours=24)
            s = yesterday.strftime(p)
            e = today.strftime(p)
            r = self.__socket.get(self.BASE_URL + 'data/' + sensorid + '/' + s + '/' + e + '/max/last/clean',
                                  headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def device_new(self, mac, name, description=''):
        payload = {'mac': mac, 'name': name, 'description': description}
        try:
            r = self.__socket.post(self.BASE_URL + 'device', json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            pass
        return False, {}

    def raw_data(self, sensorid, start_date, end_date):
        p = '%Y-%m-%dT%H:%M:%S'
        s = start_date.strftime(p)
        e = end_date.strftime(p)
        try:
            r = self.__socket.get(self.BASE_URL + 'data/' + sensorid + '/' + s + '/' + e + '/raw',
                                  headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def devices_read(self):
        try:
            r = self.__socket.get(self.BASE_URL + 'devices', headers=self.__apiheader, verify=False)
            if r.status_code == 200:
                return True, r.json()
        except:
            pass
        return False, {}

    def device_read(self, deviceId):
        try:
            r = self.__socket.get(self.BASE_URL + 'device/' + deviceId, headers=self.__apiheader, verify=False)
            if r.status_code == 200:
                return True, r.json()
        except:
            pass
        return False, {}

