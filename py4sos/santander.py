"""
Reads JSON files and uploads sensor data to the 52North SOS implementation. Code is tuned to work with data from SmartSantander project: http://www.smartsantander.eu/
It formats data into the body of POST request using JSON, and  uploads data into a SOS with a JSON binding.
A JSON file with the following structure is required:
{"markers": [ {"id": "anyvalue", "anyelement": "anyvalue",... , ..."tags": "from an specified list of tags"}]}
Permission to perform transactional operations should be enable in the SOS.
Redundant data (e.g., data with the same Id and time stamp) is ignored.

It includes several options to upload data using multiple threads. This may crash the service if it cannot handle all request, this depends on the rubustness of the server and the SOS implementation itself.

Create: May 25, 2017
Author: Manuel G. Garcia

"""

import json as json
import os
import re
import glob
import datetime
import requests
import concurrent.futures
import time as time_
from . import wrapper
from . import transactional

# OM_types dictionary
om_types = {"m": "OM_Measurement",
            "co": "OM_CategoryObservation",
            "cto": "OM_CountObservation",
            "to": "OM_TextObservation",
            "go": "OM_GeometryObservation",
            "tho": "OM_TruthObservation"}


def num(s):  # Necessary to convert longitude and latitude from a string to a number.
    """
    Convert string into a number (float or integer)
    :param s: string containing only digits
    :return: float or integer
    """
    try:
        return int(s)
    except ValueError:
        return float(s)


def timeFromFile(filename=str):
    """
    Extract the date and time which is part of a file name. Ex: 'data_stream-2016-07-21T135509.json'. Use when time is not reported for each sensor.
    :param filename: string which contains a date and time
    :return: time stamp in ISO format
    """
    time_st = re.findall(r"\d\d\d\d[-]\d\d[-]\d\d[T]\d+", filename)
    iso_time = time_st[0][:10] + " " + time_st[0][11:13] + ":" + time_st[0][13:15] + ":" + time_st[0][15:]

    return iso_time


def history(hist_directory):
    '''
    Opens a history file or creates a new one at root directory.
    A history file keeps a record of which sensors and observations have been processed
    File structure = {node: {count: int, latest: time_of_last_observation}}
    :param hist_directory: path to directory to store history files.
    :return: The newest history file in root directory OR
            an empty history file
    '''

    os.chdir(hist_directory)  # set Windows directory

    try:
        # load latest modified history file
        newest = max(glob.iglob('*.json'), key=os.path.getmtime)
        his = open(newest)
        pool = json.load(his)
        his.close()
    # when no history file is found
    except ValueError:  # on empty directory
        # raise message
        print('------------------------------------')
        print('WARNING!:')
        print('Empty directory for history files')
        print('Starting new record')
        print('------------------------------------')
        pool = {}  # start an empty dictionary for history
    return pool


def loadData(root_directory, file_name, nest='markers'):
    '''
    Opens and reads a json file in the root directory
    :param root_directory: path to directory containing json files.
    :param file_name: file name
    :param nest: key name of the most upper object in the JSON file, which is an array. Default 'markers'
    :return: dictionary containing json objects
    '''
    # open file
    with open(root_directory + file_name) as f:
        jdata = json.load(f)
        jdata = jdata[nest]
    return jdata


def cleanData(objectlist, has_tag=str, time_attrib=True):
    """
    Check if objects in a list contains elements: 'id', georeference, valid time and tags.
    :param objectlist: a list containing valid JSON objects. As returned by loadData function.
    :param has_tag: objects with this tag name will be kept. The rest will be removed.
    :param time_attrib: When True a time attribute check will be ignored.
    :return: a list of json objects
    """
    # counter for removed objects
    i = 0
    cleanList = []
    for o in objectlist:
        # Keep objects with key 'id' and tag = has_tag
        # Keep objects with georeference, e.g. 'longitude' not null and 'longitude'/'latitude' is not zero.
        if ('id' in o
            and o["longitude"] is not None and
                    o["tags"] == has_tag and
                    num(o["longitude"]) != 0.0 and
                    num(o["latitude"]) != 0.0):
            # filter based on valid time.
            if time_attrib is True:
                try:
                    if 'Last update' in o:
                        reported_time = o['Last update']  # reported time
                    else:
                        reported_time = o['LastValue']  # Another key for time (in waste collector)
                except KeyError:  # When object has not this key
                    print("*** Object has no 'Time' attribute ***")
                    continue  # Go to the next object
                else:  # if object has time attribute
                    # Filter  zero time
                    if reported_time != '0000-00-00 00:00:00':  # Sensors/observations will be added
                        # only when objects hold a valid time
                        cleanList.append(o)  # add to  clean list.
            else:  # when time attribute is false
                cleanList.append(o)
        else:
            # print('------------------------')
            # print("!!!No 'id' name in object: " + str(i))
            # print('------------------------')
            i += 1  # increase counter
    print(str(i) + ' Objects were removed!')

    return cleanList


class Sos():
    def __init__(self, url, token=''):
        self.sosurl = str(url)  # url to access the SOS
        self.token = str(token)  # security token, optional
        # Test if URL exists
        try:
            test = requests.get(self.sosurl)
            # TODO: test for token authorization
            test.raise_for_status()
        except requests.HTTPError:
            print("The URL is not valid")


def upload_directory2sos(sos, directory, sensor_type, history_path, threads=1, time_attribute=True,
                         spatial_profile=True):
    """
    Parses all JSON files in a directory, prepares SOS requests for registering sensors and observations, and uploads data to an existing SOS.
    Application is limited by an intense use of memory when a directory contains a very large number of files.
    The use of multi-thread  may crash the SOS. To limit the number of crashes, the function will stop for 20 seconds every after every 50 files.
    :param sos: Object describing an existing SOS with valid URL and token.
    :param directory: path to the directory which contains a JSON file.
    :param sensor_type: the type of sensors for which requests will be prepare (e.g., 'light', 'weather_station', etc.)
    :param history_path: path to directory for history logs
    :param threads: number of threads for multi-thread uploading. Default is 1 thread.
    :param time_attribute: states if specific sensor type contains a time attribute or not. Default is True.
    :param spatial_profile: switches between the use of insertObservationSP (True) to insertObservation (False).
    :return: None
    """

    json_files = os.listdir(directory)  # list all files in directory
    counter = 0  # initiate counter for monitoring progress
    start_time = datetime.datetime.now()
    print('=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/')
    print('Process stated at: ', str(start_time))
    print('PROCESSING all files in directory: ', directory)

    for f in sorted(json_files):  # loop over json files. Files sorted by name.
        print('---->>Working on file: ', f)
        print('    >> Parsing file ', str(counter + 1), ' out of: ', str(len(json_files)))
        print('----------------------------------------------------------')
        # Load data from JSON file and prepare requests
        request_collection = requests_from_file(directory, f, sensor_type, history_path, time_attribute,
                                                spatial_profile)

        upload2sos(sos, request_collection, history_path, threads)
        counter += 1

        # Put the program to sleep after processing 'n' files.
        n = 50
        if counter > 0 and (counter % n) == 0:
            wait_time = 20  # time in seconds
            # print('=============================================')
            print('\n       >>>   The monkey is tired   <<<      ')
            print('            *********************            ')
            print('            **                 ** ')
            print('            **   GETTING MORE  **            ')
            print('            **      BANANAS    **            ')
            print('            **                 **            ')
            print('            *********************            ')
            print('         >>>   Wait ', wait_time, ' seconds ', '  <<<     ')
            # print('=============================================')
            time_.sleep(wait_time)
        else:
            pass

    end_time = datetime.datetime.now()
    elapse_t = end_time - start_time
    print('------------------------------')
    print('>> Directory Upload Complete <<')
    print('-> Total upload time: ' + str(elapse_t))
    print('------------------------------')

    return None


def requests_from_file(directory, file_name, sensor_type, hist_path, time_attrib=True, spatial_profile=True):
    """
    Parse a single JSON file and prepare SOS requests for registering sensors and observations.

    :param directory: path to the directory which contains a JSON file
    :param file_name: name of a JSON file containing sensor data
    :param sensor_type: the type of sensors for which requests will be prepare (e.g., 'light', 'weather_station', etc.)
    :param hist_path: path to directory for history logs
    :param time_attrib: states if specific sensor type contains a time attribute or not. Default is True.
    :param spatial_profile: switches between the use of insertObservationSP (True) to insertObservation (False).
    :return: a list of valid requests, and up-to-date history log
    """

    print('=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/')
    print('PROCESSING a single file: ', file_name)
    print('----------------------------------------------------------')

    jdata = loadData(directory, file_name)

    # ------------------------------
    # Parsing Parameters:
    # ------------------------------
    # sensor type
    type_sensor = wrapper.SensorType(sensor_type)
    sensor_attrib = type_sensor.pattern['attributes']
    # parsing history
    hist = history(hist_path)
    # Remove invalid objects
    # print(type_sensor.pattern['name'])
    clean_obj = cleanData(jdata, type_sensor.pattern['name'], time_attrib)

    #  Chose Insert Observation function:
    if spatial_profile is True:
        insertobservation = transactional.insertObservationSP  # with Transactional Profile
    else:
        insertobservation = transactional.insertObservation  # without it

    # Chose Inser Sensor function:
    if type_sensor.pattern['type'] == 'mobile':
        insertsensor = transactional.insertMobileSensor
    else:
        insertsensor = transactional.insertSensor

    prepared_requests = []  # request collector
    for o in clean_obj:  # loop over each object in input file
        ide = o['id']
        if ide in hist:
            # if node was previously processed
            # fetch time:
            if time_attrib:
                t = o['Last update']
            else:
                # TODO: time needs transformation wrt server-time
                t = timeFromFile(file_name)  # get time form file name

            if t not in hist[str(ide)]["times"]:  # check if object has new time
                # change time format
                tt = t.split()
                time = tt[0] + 'T' + tt[1] + '+00:00'
                body = wrapper.Batch(ide)  # initiate batch instance

                for a in sensor_attrib:  # loop over each attribute

                    # OM type
                    om = type_sensor.om_types[a[1]]
                    # define procedure
                    procedure = wrapper.Procedure(ide, a[0], 'http://www.geosmartcity.nl/test/observableProperty/', om)
                    off_name = 'offering for ' + ide + '_' + type_sensor.pattern['name']
                    # WARNING: defining an offering for each node
                    offering = wrapper.Offering('http://www.geosmartcity.nl/test/offering/', ide, off_name)
                    # Indexing observation identifier
                    # Index := ide_count+1
                    new_ide = ide + '_' + "".join(a[0].split()) + '_' + str(
                        hist[ide]['count'] + 1)  # ID like: ide_(count +1)

                    observation = wrapper.Observation(new_ide)
                    observation.uom = om
                    observation.phTime, observation.rTime = time, time  # same time for both
                    # fetch observation value

                    # Skip 'Location' attribute
                    if a[1] != "go":

                        # Get value for attribute in TypeSensor object
                        try:
                            val = str(o[a[0]])
                            # get numeric value
                        except KeyError:  # when key doesn't exits in object
                            continue  # Skip request for this attribute
                        val_num = re.findall(r"[-+]?\d*\.\d+|\d+", val)

                        # change to float data type
                        if len(
                                val_num) == 0:  # Node attribute had not data, or reported an empty (regarded as Null) value.
                            print('Empty val_num for: ' + str(ide))
                            if om == "OM_Measurement":
                                observation.Value = -9.99  # alternative 'null' value for 'float' types
                            elif om == "OM_CountObservation":
                                observation.Value = -1111  # alternative 'null' value for 'integer' type
                            else:  # TODO: add more alternative values
                                continue
                            # Fetch magnitude
                            unit = ''.join([i for i in val if not i.isdigit()])
                            for character in [" ", ".", "-"]:
                                unit = unit.replace(character, "")
                            observation.unit = unit
                        else:
                            observation.Value = num(val_num[0])  # value of the observation
                            # Fetch magnitud for the value:
                            # observation.unit = re.sub((val_num[0] + ' '), "", val, count=1)
                            unit = ''.join([i for i in val if not i.isdigit()])
                            for character in [" ", ".", "-"]:
                                unit = unit.replace(character, "")
                            observation.unit = unit

                    else:  # For goemetry observation
                        observation.Value = None
                        observation.unit = None

                    # feature of interest:
                    coord = (float(o['longitude']), float(o['latitude']), -9.99)  # No data := -9.99
                    foi = wrapper.FoI('degree', 'm', coord, ide)
                    # prepare body request
                    if type_sensor.pattern["type"] == "mobile":
                        body_obs = insertobservation(observation, foi, offering, procedure, a[0])
                    else:
                        body_obs = insertobservation(observation, foi, offering, procedure, a[0])
                    body.add_request(body_obs)  # collect insert observation request

                prepared_requests.append(body)

                # After insert observation (parsing) is successful
                # update sensor history
                old_val = hist[ide]["count"]
                hist[ide]["count"] = old_val + 1  # update counter
                hist[ide]["times"].append(t)  # store new time
            else:
                continue

        else:
            # if node is new in hist
            # insert sensor to SOS
            # print('NEW ' + type_sensor.pattern['name'] + ' SENSOR for: ' + str(ide))
            # phenomena / result time:
            if time_attrib:
                try:
                    t = o['Last update']
                except KeyError:
                    t = o['LastValue']  # special case (waste collector)

            else:
                # TODO: time needs transformation wrt server-time
                t = timeFromFile(file_name)  # get time form file name

            # change time format
            tt = t.split()
            time = tt[0] + 'T' + tt[1] + '+00:00'

            body_sensor = ""

            # Start batch instance
            body = wrapper.Batch(ide)

            # Prepare Sensor Registration:
            for a in sensor_attrib:

                # OM type
                om = type_sensor.om_types[a[1]]
                # print(type_sensor.om_types[a[1]])

                # define procedure
                procedure = wrapper.Procedure(ide, a[0], 'http://www.geosmartcity.nl/test/observableProperty/', om)
                off_name = 'offering for ' + ide + '_' + type_sensor.pattern['name']
                # WARNING: defining an offering for each node
                offering = wrapper.Offering('http://www.geosmartcity.nl/test/offering/', ide, off_name)

                # feature of interest:
                try:
                    coord = (float(o['longitude']), float(o['latitude']), -9.99)  # No data := -9.99
                except TypeError:
                    print(o)

                # Feature of interest
                foi = wrapper.FoI('degree', 'm', coord, ide)

                # prepare body for insert sensor
                body_sensor = insertsensor(offering, procedure, foi, type_sensor)
            body.add_request(body_sensor)  # append insert sensor request

            # Prepare Insert Observation Requests:
            cuenta = 0

            for a in sensor_attrib:
                # OM type
                om = type_sensor.om_types[a[1]]
                # define procedure
                procedure = wrapper.Procedure(ide, a[0], 'http://www.geosmartcity.nl/test/observableProperty/', om)
                off_name = 'offering for ' + ide + '_' + type_sensor.pattern['name']
                # WARNING: defining an offering for each node
                offering = wrapper.Offering('http://www.geosmartcity.nl/test/offering/', ide, off_name)

                # Indexing observation identifier
                new_ide = ide + '_' + "".join(a[0].split()) + '_1'  # ID like: ide_(count +1)
                # print("id: "+new_ide)
                observation = wrapper.Observation(new_ide)
                observation.uom = om
                observation.phTime, observation.rTime = time, time  # same time for both

                # Skip 'Location' attribute
                if a[1] != "go":

                    # fetch observation value
                    try:  # Avoid stop, when sensor type report different attributes
                        val = str(o[a[0]])
                        # get numeric value
                    except KeyError:
                        # print('Sensor without this attribute..!!!' + ' Sensor: ' + str(ide) +' attribute: ' + str(a))
                        continue
                    val_num = re.findall(r"[-+]?\d*\.\d+|\d+", val)
                    # change to float data type
                    # print("type of measurement:", om)
                    if len(val_num) == 0:
                        print('Empty val_num for: ' + str(ide))
                        if om == "OM_Measurement":
                            observation.Value = -9.99  # alternative 'null' value for 'float' data type in Database
                        elif om == "OM_CountObservation":
                            observation.Value = -1111  # alternative 'null' value for 'integer' data type in Database
                        else:  # TODO: add more alternative values
                            continue
                        # Fetch magnitude
                        # When No magnitude an empty string is returned.
                        unit = ''.join([i for i in val if not i.isdigit()])
                        for character in [" ", ".", "-"]:
                            unit = unit.replace(character, "")
                        # print("unit at: ", o["id"], " for ", a, " is: ", unit)
                        observation.unit = unit  # val should contain only the magnitud

                    else:
                        observation.Value = num(val_num[0])  # value of the observation
                        # Fetch magnitud for the value:
                        # observation.unit = re.sub((val_num[0] + ' '), "", val, count=1)
                        unit = ''.join([i for i in val if not i.isdigit()])
                        for character in [" ", ".", "-"]:
                            unit = unit.replace(character, "")
                        # print("unit at: ", o["id"], " for ", a, " is: ", unit)
                        observation.unit = unit
                else:  # For goemetry observation
                    observation.Value = None
                    observation.unit = None

                # feature of interest:
                coord = (float(o['longitude']), float(o['latitude']), -9.99)  # No data := -9.99
                foi = wrapper.FoI('degree', 'm', coord, ide)

                # insert observation to SOS
                if type_sensor.pattern["type"] == "mobile":
                    # TODO: modify insert sensor function for mobile sensors
                    body_obs = insertobservation(observation, foi, offering, procedure, a[
                        0])  # TODO: Fix, this will produce an error if a mobile sensor is declared
                else:
                    body_obs = insertobservation(observation, foi, offering, procedure, a[0])
                body.add_request(body_obs)  # add observation request
                cuenta += 1
            prepared_requests.append(body)

            # After sensor and observation are successful
            # Update sensor history with new record

            hist[ide] = {"count": 1, "times": [t]}

    # insert parsing history. TODO: Is this necessary?
    # hist["last parsed"] = {"runtime error": {}, "file name" : '', "run time": ''}

    return {"requests": prepared_requests, "history": hist, "file": file_name}


def upload2sos(sos, request_collection, hist_path, threads=1):
    """
    Upload data to a SOS using HTTP POST requests
    :param sos: Object describing an existing SOS
    :param request_collection: dictionary containing: HTTP requests, historic log, and name parsed file. Each request is an instance of Batch class
    :param hist_path: directory in which the history log files will be saved
    :param threads: number of threads for multi-thread uploading. Default is 1 thread.
    :return: None
    """
    #  TODO: currently it work for a single file (a list of Batch objects). Estend it to  deal with multiple files and including a 'sleep' time might not be of practical case.
    # If new requests were created
    err_log = {}  # initiate error log
    num_posts = len(request_collection['requests'])  # number of requests
    hist = request_collection['history']
    file_name = request_collection['file']
    re_quests = request_collection['requests']

    start_time = datetime.datetime.now()

    if num_posts > 0:
        # send requests
        print('-----------------------------')
        print('UPLOADING DATA TO: ' + sos.sosurl)
        print('SENDING ', str(num_posts), ' REQUESTS...', 'Using:', str(threads), 'threads')
        print('WARNING: Uploading redundant data won"t be flagged', '...working to fix it...')
        # wrapper.sosPost(my_requests[count].reqs(), url, token, response=False)
        # my_requests.clear()
        # count += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_req = {executor.submit(wrapper.sosPost, reques.reqs(), sos.sosurl, sos.token, True): reques for
                             reques in re_quests}
            for future in concurrent.futures.as_completed(future_to_req):
                req = future_to_req[future]  # Batch instances
                # print(future.result())
                try:
                    # TODO: server is not reporting errors when sending redundant data. Check sosPost as well.
                    #     # print('hello')
                    future.result()
                # # future.result()
                except Exception as exc:
                    future.exception()
                    err_log[str(datetime.datetime.now())] = [req.id, exc, req.body]
                    print('%r generated an exception: %s Request: %s' % (req.id, exc, req.body))

        e_time = datetime.datetime.now()

        wait = e_time - start_time
        reqs_per_post = len(re_quests)  # an aproximation
        # print('Upload time: ', file_name, str(wait), )
        print('SOS server load: ', str(round(reqs_per_post / wait.total_seconds(), 1)), 'Rps')
        # print('Accumulated time: ', str(datetime.datetime.now() - start_time))
        print('------------------------------')
        request_collection.clear()
        # count += 1

        # update history log file:
        updateHistory(hist_path, file_name, hist, err_log)

    # report not new requests were send
    else:
        print("*** No NEW sensors nor  NEW observations in file %r ***" % file_name)
        # count += 1

    # Create  error log file if any error are reported during uploading
    if len(err_log) > 0:
        # file name
        efile = 'runtime-errors' + datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S") + '.log'
        ef = open(hist_path + efile, 'w')  # save to same directory as history log files
        json.dump(err_log, ef)
        ef.close()

    end_time = datetime.datetime.now()
    elapse_t = end_time - start_time
    print('------------------------------')
    print('File Upload Complete')
    print('Upload time: ' + str(elapse_t))
    print('------------------------------')

    return None


def updateHistory(hist_path, file_name, latest_history_log, error_log):
    """
    Updates the history log of requests sent to the SOS server. It writes a new file containing the latest changes to a local directory.
    If errors in he server occurred, an error log will be added to the history file
    :param hist_path: path a directory to store the new (updated) history log file
    :param file_name: name of the source file which is uploading.
    :param latest_history_log: up to date history log, formatted as JSON
    :param error_log: error reports. Formatted as JSON
    :return: new history log file formatted as JSON
    """
    hist = latest_history_log
    hist['last upload'] = {"name": file_name, "run time": str(datetime.datetime.now()), "runtime error": error_log}
    fname = 'hist-' + datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S") + '.json'
    fn = open(hist_path + fname, 'w')  # create new history file
    json.dump(hist, fn)  # write to file
    fn.close()
    print("History log file was updated!!")
    return None

#  TODO: URI from waste sensors are not valid. They contain spaces and special characters. They have to be remove


def main():

    #  TODO: Write some tests
    # dir = 'c:/sos_santander/raw_data/sample/'
    # f_name = "santander_example_data.json"
    # f_name2 = "data_stream-2016-07-01T080007.json"
    # h_dir = 'c:/Temp/hist_temp/'

    url = 'http://130.89.217.201:8080/sos-4.4/service'
    token = 'TWFudWVsIEdhcmNpYQ=='
    # rq = requests_from_file(dir, f_name, 'bus', h_dir, time_attrib=True)

    # r = rq["requests"][0]
    # print(r.reqs())

    sos = Sos(url, token)

    # upload2sos(sos, rq, h_dir, 3)

    # upload_directory2sos(sos, dir, 'light', h_dir,3)


if __name__ == '__main__':
    main()