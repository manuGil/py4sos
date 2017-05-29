""""
 Classes and funcitons to query the 52North implementation of SOS 2.0.0. Functions generate request to the SOS API using HTTP methods. Request are generated using the 'application/json' binding. Headers, parameters, and data are encoded in JSON and piped though the SOS API.
For documentation of 52North SOS visit: https://wiki.52north.org/bin/view/SensorWeb/SensorObservationServiceIVDocumentation
 Author: Manuel G. Garcia
 Created: 23-05-2017
"""

import requests


# OM Measurement types:
class OMtype():
    a = "OM_Measurement"
    b = "OM_CategoryObservation"
    c = "OM_CountObservation"
    d = "OM_TextObservation"
    e = "OM_GeometryObservation"
    f = "OM_TruthObservation"

    def __init__(self):
        print('OMtype.a --> OM_Measurement')
        print('OMtype.b --> OM_CategoryObservation')
        print('OMtype.c --> OM_CountObservation')
        print('OMtype.d --> OM_TextObservation')
        print('OMtype.e --> OM_GeometryObservation')
        print('OMtype.f --> OM_TruthObservation')

class Observation:
    def __init__(self, identifier):
        self.id = identifier  # usually an integer

    def values(self, OM_type, value, units, phenomenon_time, result_time, ):
        self.uom = OM_type  # unit of measurement. OM unit type.
        self.Value = value  # value of phenomenon
        self.phTime = phenomenon_time  # time of the phenomenon
        self.rTime = result_time  # time of the result
        self.unit = units  # magnitude for value



# class defining the values for an offering
class Offering:

    def __init__(self, rooturl, offeringId, offeringname):
        self.url = rooturl
        self.id = offeringId
        self.name = '\"' + str(offeringname) + '\"'
        self.fullId = str(rooturl) + str(offeringId)


class Procedure:

    def __init__(self, procedure_id, property_name, prop_url, property_om, property_id='100&ABB'):
        self.name = property_name
        self.url = prop_url # Observable property url
        self.pid = procedure_id # string type id
        # for observable property ID
        if property_id == '100&ABB':
            self.obid = procedure_id # same as precedure_id
        else:
            self.obid = property_id # assign different
        self.om = property_om  # unit of measurement
        self.defn = prop_url + str(self.obid) # definition URL

# Feature of Interest
class FoI:

    def __init__(self, xy_unit, z_unit, cords, feature_id):
        '''
        :param xy_unit: unit for X,Y coordinates. Eg. degrees, meters, etc.
        :param z_unit: unit for z, usually height in meters.
        :param cords: a tuple like (X, Y, Z)
        :param feature_id:  id for the feature of interest
        '''
        self.x = cords[0]
        self.y = cords[1]
        self.z = cords[2]
        self.Vunit = str(z_unit) # unit of the vertical dimension
        self.Hunit = str(xy_unit) # unit of the horizontal dimensions
        self.fid = feature_id # ID of the feature


class SensorType:

    # In a SOS sensors can be:
        # (a) In-situ ('on the spot') or (b) remote (e.g. satellites, airborne)
        # (1) stationary (with fixed location) or mobile (in movement). Classification used in this class.
        # TODO: extend class to consider all types.

    om_types = {"m": "OM_Measurement", "co": "OM_CategoryObservation", "cto": "OM_CountObservation",
                "to": "OM_TextObservation", "go": "OM_GeometryObservation", "tho": "OM_TruthObservation", "xo": "OM_ComplexObservation"}

    def __init__(self, type_): # type refers to the description of phenomena observed,
                                #  and the mobility of the the sensor.
            # TODO: work on an ontology to deal with different phenomena names
        if type_ == "light":  # LIGHT
            self.pattern = {"name": "light", "type": 'fixed', "attributes": [("Luminosity", "m"), ("Battery level", "m"), ("Temperature","m")]}
        elif type_ == "bus":  # BUS
            self.pattern = {"name": "BUS", "type": 'mobile', "attributes": [("Speed", "m"), ("Course", "m"), ("Odometer","m"), ("CO", "m"), ("Particles","m"), ("Ozone N02","m"), ("N02","m"), ("Temperature","m"), ("Humidity","m")]} # ("Location","go")]}
        elif type_ == "env_station":  # ENV_STATION
            self.pattern = {"name": "env_station", "type": 'fixed', "attributes": [("Battery level","m"), ("Temperature","m"), ("Relative humidity","m"), ("Soil Moisture","m"), ("Solar Radiation","m"), ("Rainfall","m"), ("Wind_Speed","m"), ("Wind_Direction","m"), ("Radiation_PAR","m"), ("Atmospheric Pressure","m")]}
        elif type_ == "irrigation":  # IRRIGATION
            self.pattern= {"name": "irrigation", "type": 'fixed', "attributes": [("Battery level","m"), ("Temperature","m"), ("Relative humidity", "m"), ("Soil Moisture", "m"), ("Soil Temperature", "m")]}
        elif  type_ == "agriculture":  # AGRICULTURE
            self.pattern = {"name": "agriculture", "type": 'fixed', "attributes": [("Battery level", "m"), ("Temperature", "m"), ("Relative humidity", "m")]}
        elif type_ == "noise":  # NOISE
            self.pattern = {"name": "noise", "type": 'fixed', "attributes": [("Battery level", "m"), ("Noise", "m")]}
        elif type_ == "vehicle_counter":  # VEHICLE_COUNTER
            self.pattern = {"name": "vehicle_counter", "type": 'fixed', "attributes": [("Occupancy", "m"), (" Count", "cto")]}
        elif type_ == "vehicle_speed":  # VEHICLE_SPEED
            self.pattern = {"name": "vehicle_speed", "type": 'fixed', "attributes": [("Occupancy", "m"), (" Count", "cto"), (" Average Speed",  "m"), (" Median Speed",  "m")]}
        elif type_ == 'temp':  # TEMP
            self.pattern = {"name": "temp", "type": 'fixed', "attributes": [("Battery level", "m"), ("Temperature", "m")]}
        elif type_ == 'outdoor': # Low EMF, measuring 'electrosmog'
        # EFM-project, http://lexnet-project.eu/
            self.pattern = {"name": "outdoor", "type": 'fixed', "attributes": [(" EField (900 Mhz)", "m"), (" EField (1800 Mhz)", "m"), (" EField (2100 Mhz)", "m"), (" EField (2400 Mhz)", "m")]}
        elif type_ == 'waste': # WASTE COLLECTOR (Truck)
            self.pattern = {"name": "waste", "type": "fixed", "attributes": [("temperature", "m"), ("humidity", "m"), ("particles", "m"), ("CO", "m"), ("NO2","m"), ("O3", "m"), ("Location","go")]}
        elif type_ == 'air': # AIR, Not currently reporting
            self.pattern = {"name": "air", "type": "fixed"}
        else:
            print("Sensor type is not defined")

class Batch:
    # Container for prepared request for a SOS
    def __init__(self, id_):
        self.id = id_
        self.request_list = []
        self.body = {"service": "SOS", "version": "2.0.0", "request": "Batch",  "requests": self.request_list} # "stopAtFailure": True,
    def add_request(self, request):
        self.request_list.append(request)
    def reqs(self):  # output for
        return self.body


def sosPost(body, url, token, response=False):
    '''
    Sends a transaction request to a SOS using POST
    :param body: JSON formatted data describing an observation, its properties and values. See <obs_example.json>
    :param token: Authorization Token from the server side.
    :param url: URL to the endpoint where the SOS with transactional capabilites is listening.
    :param response: If True, it prints the full response received from the server.
    '''

    # Add headers:
    headers = {'Authorization': str(token), 'Accept': 'application/json'}

    query = requests.post(url, headers=headers, json=body) # json=body)

    # print(query.json())
    # print(query.status_code)
    # TODO: when redundant data is passed, function report status code 200.
    if query.status_code != 200:

        # report SOS errors:
        if 'exceptions' in query.json():
            print('Exception at SOS:')
            print('Server Status Code: ' + str(query.status_code))
            print('REPORT@sosPost(): ')
            for i in query.json()['exceptions']:
                print(i)

        # for any HTTP error:
    return query.raise_for_status()


def sosSoapPost(body, url, token, response=False):  # TODO: To be completed and debug
    '''
    Sends a transaction request to a SOS SOAP biding.
    :param body: XML formatted body of the request.
    :param token: Authorization Token from the server side.
    :param url: URL to the endpoint where the SOS with transactional capabilites is listening.
    :param response: If True, it prints the full response received from the server.
    '''

    # Add headers:
    headers = {'Authorization': str(token), 'Content-Type': 'application/soap+xml'}

    query = requests.post(url, headers=headers, data=body) # json=body)
    # , 'Content-Type': 'application/soap+xml'

    if query.status_code != 200:

        # report SOS errors:
        print(query.text)

        # for any HTTP error:
    return query.raise_for_status()




