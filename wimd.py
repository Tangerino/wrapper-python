'''
This is the WIMD.IO API wrapper
'''
import datetime
from datetime import timedelta
import sys
import requests

class wimd(object):
    BASE_URL = "http://wimd.io/v2/"
    #BASE_URL = "http://192.168.0.27/v2/"

    def __init__(self):
        requests.packages.urllib3.disable_warnings()
        self.__socket = requests.session()
        self.__apikey = ''
        self.__apiheader = {'apikey': ''}
        self.__permissions = 0

    def url(self):
        return self.BASE_URL

    def apikey(self):
        return self.__apikey

    def is_admin(self):
        if self.__permissions & 0x80:
            return True
        else:
            return False

    def can_create(self):
        return True

    def can_read(self):
        return True

    def can_update(self):
        return True

    def can_delete(self):
        return True

    def login(self, email, password):
        rc = False
        payload = {'email': email, 'password': password}
        try:
            r = self.__socket.post(self.BASE_URL + 'account/login', json=payload, verify=False)
            if r.status_code == 200:
                resp = r.json()
                self.__apikey = resp['apikey']
                self.__apiheader = {'apikey': self.__apikey}
                self.__permissions = resp['permissions']
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

    def place_new(self, name, description=''):
        payload = {'name': name, 'description': description}
        try:
            r = self.__socket.post(self.BASE_URL + 'place', json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}

    def place_delete(self, place_id):
        try:
            r = self.__socket.delete(self.BASE_URL + 'place/' + place_id, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}


    def thing_new(self, place_id, name, description=''):
        payload = {'name': name, 'description': description}
        try:
            r = self.__socket.post(self.BASE_URL + 'place/' + place_id + "/thing", json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            pass
        return False, {}

    def things_read(self, place_id):
        try:
            r = self.__socket.get(self.BASE_URL + 'place/' + place_id + '/things', headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def thing_read(self, thing_id):
        try:
            r = self.__socket.get(self.BASE_URL + 'thing/' + thing_id, headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            pass
        return False, {}

    def thing_update(self, thing_id, place_id, name, description):
        payload = {'placeid': place_id, 'name': name, 'description': description}
        try:
            r = self.__socket.put(self.BASE_URL + 'thing/' + thing_id, json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def thing_link_sensor(self, thing_id, sensor_id):
        try:
            r = self.__socket.post(self.BASE_URL + 'thing/' + thing_id + "/link/" + sensor_id, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def thing_unlink_sensor(self, thing_id, sensor_id):
        try:
            r = self.__socket.delete(self.BASE_URL + 'thing/' + thing_id + "/link/" + sensor_id, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def thing_delete(self, thing_id):
        try:
            r = self.__socket.delete(self.BASE_URL + 'thing/' + thing_id, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
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
            return False, {sys.exc_info()[0]}
        return False,{}

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

    def sensor_rule_read(self, sensor_id):
        try:
            r = self.__socket.get(self.BASE_URL + 'sensor/' + sensor_id + "/rule", headers=self.__apiheader, verify=False)
            return r.status_code == 200, r.json()
        except:
            return False, {sys.exc_info()[0]}

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

    def device_update(self, deviceid, mac, name, description=''):
        payload = {'mac': mac, 'name': name, 'description': description}
        try:
            r = self.__socket.put(self.BASE_URL + 'device/' + deviceid, json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def device_delete(self, deviceid):
        try:
            r = self.__socket.delete(self.BASE_URL + 'device/' + deviceid, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def etl_new(self, name, endpoint, username, password, type, placeid, database, table):
        payload = {"name":name, "endpoint":endpoint, "username":username, "password":password, "type":type, "placeid":placeid, "database":database, "table":table}
        try:
            r = self.__socket.post(self.BASE_URL + 'etl', json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def etls_read(self):
        try:
            r = self.__socket.get(self.BASE_URL + 'etls', headers=self.__apiheader, verify=False)
            if r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def etl_read(self, etl_id):
        try:
            r = self.__socket.get(self.BASE_URL + 'etl/' + etl_id, headers=self.__apiheader, verify=False)
            if r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def etl_update(self, etl_id, name, endpoint, username, password, type, placeid, database, table):
        payload = {"name":name, "endpoint":endpoint, "username":username, "password":password, "type":type, "placeid":placeid, "database":database, "table":table}
        try:
            r = self.__socket.put(self.BASE_URL + 'etl/' + etl_id, json=payload, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

    def etl_delete(self, etl_id):
        try:
            r = self.__socket.delete(self.BASE_URL + 'etl/' + etl_id, headers=self.__apiheader, verify=False)
            if r.status_code == 201 or r.status_code == 200:
                return True, r.json()
        except:
            return False, {sys.exc_info()[0]}
        return False, {}

