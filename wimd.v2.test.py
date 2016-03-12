from wimd import wimd
import random

USER_LOGIN = ''
USER_PWD   = ''

def delete_all_devices (w):
    rc, devices = w.devices_read()
    if rc == False:
        return rc, devices,'devices_read'
    for device in devices:
        rc, resp = w.device_delete(device['id'])
        if rc == False:
            return rc,resp
    return True, {}

def run_device_tests(w):
    rc, j = delete_all_devices(w)
    if rc == False:
        return rc, j, "Delete device by name"
    rc, d = w.device_new('python_mac','python_device','python_description')
    if rc == False:
        return rc, d,'create_device'
    rc, devices = w.devices_read()
    if rc == False:
        return rc, devices,'devices_read'
    for device in devices:
        if device['mac'] == 'python_mac':
            rc, dx = w.device_read(device['id'])
            if rc == False:
                return rc, dx,'device_read'
            d2 = dx[0]
            if d2['mac'] != device['mac']:
                return rc, d2,'device_read->bad MAC'
            rc, d3 = w.device_update(d2['id'], 'python_mac', 'new_name','new description')
            if rc == False:
                return rc, d3,'update_device'
            rc, d4 = w.device_read(device['id'])
            if rc == False:
                return rc, d4,'device_read'
            if d4[0]['name'] != 'new_name':
                return rc, d4,'device_update->bad name'
            rc, resp = w.device_delete(d4[0]['id'])
            if rc == False:
                return rc,resp, 'Delete device'
            return True, {}, "All tests OK for DEVICE"
    return rc, {},'device not found'

def find_first_place(w):
    rc, places = w.places()
    if rc == False:
        return  rc, places
    if len(places) > 0:
        return True, places[0]['id']
    return False, "No available places"

def delete_all_etls (w, name):
    rc, etls = w.etls_read()
    if rc == False:
        return rc, etls,'etls_read'
    for etl in etls:
        rc, resp = w.etl_delete(etl['id'])
        if rc == False:
            return rc,resp
    return True, {}

def run_etl_tests(w):
    rc, resp = delete_all_etls(w, "MY_ETL_TEST")
    if rc == False:
        return rc, resp, "delete_etl_by_name"
    rc, place = find_first_place(w)
    if rc == False:
        return rc, place, "find_first_place"
    rc, etl = w.etl_new("MY_ETL_TEST","endpoint","username","password",1,place,"dbname","tablename")
    if rc == False:
        return rc, etl, "etl_new"
    rc, etls = w.etls_read()
    if rc == False:
        return rc, etls, "etls_read"
    for etl in etls:
        if etl['name'] == "MY_ETL_TEST":
            rc, resp = w.etl_update(etl['id'],etl['name'],"newendpoint","username","password",1,place,"dbname","tablename")
            if rc == False:
                return rc, resp, "etl_update"
            rc, resp = w.etl_delete(etl['id'])
            return rc, resp, "etl_delete"
    return False, {}, "etl not found"

def delete_all_things (w, place, name):
    rc, things = w.things_read(place)
    if rc == False:
        return rc, things,'etls_read'
    for thing in things:
        print("Deleting thing " + name + ". ID:" + thing['id'])
        rc, resp = w.thing_delete(thing['id'])
        if rc == False:
            return rc,resp
    return True, {}

def run_thing_tests(w):
    rc, place = find_first_place(w)
    if rc == False:
        return rc, place, "find_first_place"
    rc, resp = delete_all_things(w, place, "MY_THING_TEST")
    if rc == False:
        return rc, resp, "delete_thing_by_name"
    rc, thing = w.thing_new(place,"MY_THING_TEST","comment")
    if rc == False:
        return rc, thing, "thing_new"
    rc, things = w.things_read(place)
    if rc == False:
        return rc, things, "things_read"
    for thing in things:
        if thing['name'] == "MY_THING_TEST":
            rc, resp = w.thing_update(thing['id'],thing['placeid'],"MY_NEWTHING","descrip")
            if rc == False:
                return rc, resp, "thing_update"
            rc, resp = w.thing_delete(thing['id'])
            return rc, resp, "thing_delete"
    return False, {}, "thing not found"

def run_sensor_rule_tests(w):
    rc, places = w.places()
    if rc == False:
        return rc, places, "run_sensor_rule_tests"
    for place in places:
        print("Place " + place['name'])
        rc, things = w.things_read(place['id'])
        if rc == False:
            return rc, things, "run_sensor_rule_tests"
        for thing in things:
            print("Place " + place['name'] + ". Thing " + thing['name'])
            rc, sensors = w.sensors(thing['id'])
            if rc == False:
                return rc, sensors, "run_sensor_rule_tests"
            for sensor in sensors:
                print("Place " + place['name'] + ". Thing " + thing['name'] + ". Sensor " + sensor['name'])
                rc, rule = w.sensor_rule_read(sensor['id'])
                if rc == False:
                    return rc, rule, "run_sensor_rule_tests"
    return True, {}, "run_sensor_rule_tests"

def run_thing_link_sensor_tests(w):
    n = random.randint(1, 1000000)
    rn = str(n)
    rc, j = delete_all_devices(w)
    if rc == False:
        return rc, j, "Delete device by name"
    rc, d = w.device_new('python_mac','python_device','python_description')
    if rc == False:
        return rc, d,'create_device'
    rc, sensor = w.sensor_new(d[0]['devkey'],"SENSOR_" + rn,"SENSOR_" + rn,"","","","")
    if rc == False:
        return rc, sensor, "sensor_new"
    rc, place = w.place_new("PLACE_" + rn, "PLACE_" + rn)
    if rc == False:
        return rc, place, "place_new"
    rc, thing = w.thing_new(place[0]['id'], "THING_" + rn, "THING_" + rn)
    if rc == False:
        return rc, thing, "thing_new"
    rc, resp = w.thing_link_sensor(thing[0]['id'], sensor[0]['id'])
    if rc == True:
        rc, resp = w.thing_unlink_sensor(thing[0]['id'], sensor[0]['id'])
    w.device_delete(d[0]['id'])
    w.place_delete(place[0]['id'])
    if rc == False:
        return rc, resp, "thing_link_sensor"
    return True, {}, "run_thing_link_sensor_tests"


def main1():
    w = wimd()
    rc = w.login(USER_LOGIN, USER_PWD)
    if rc == True:
        rc, resp, routine = run_device_tests(w)
        if rc == True:
            rc, resp, routine = run_etl_tests(w)
            if rc == False:
                print(rc, resp,routine)
            else:
                rc, resp, routine = run_thing_tests(w)
                if rc == False:
                    print(rc, resp,routine)
                else:
                    rc, resp, routine = run_sensor_rule_tests(w)
                    if rc == False:
                        print(rc, resp,routine)
                    else:
                        rc, resp, routine = run_thing_link_sensor_tests(w)
                        if rc == False:
                            print(rc, resp,routine)
                        else:
                            print("PASSED")
    else:
        print("*** FATAL - Login error")

def main2 ():
    w = wimd()
    rc = w.login("carlos.tangerino@gmail.com", "mysecret")
    if rc == True:
        rc, resp, routine = run_sensor_rule_tests(w)
        if rc == False:
            print(rc, resp,routine)
        else:
            print("PASSED")

if __name__ == "__main__":
    main1()
