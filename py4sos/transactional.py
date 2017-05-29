"""
Function for the SOS Transactional profile.
This set of function format requests to publish and handle data in a SOS using a RESTful API.
Requests need to be passed as the body of a HTTP request to the SOS server.
When more than one syntax is allowed, requests as passed using XML version 2.0
Author: Manuel G. Garcia
Created: 23-05-2017
"""



#  TODO: give better names to URIs when inserting sensors


def insertSensor(offering, procedure,  foi, sensor_type):
    """
    Prepares the body of a InsertSensor request for JSON biding.
    :param offering: an instance of class Offering.Type object.
    :param Procedure: instance of class Procedure. type object.
    :param foi: feature of interest. Instance of FoI
    :param sensor_type: SensorType object
    :return: valid body for an InsertSensor request.
    """
    # for JSON double quotes: \", or \u0022
    #  specify procedure ID:
    procedureID = 'http://www.geosmartcity.nl/test/procedure/' + str(procedure.pid) # URL format
    shortName = 'short name' #string
    longName = 'long name' #string

    # Offering values
    offName = offering.name #Offering name, double quoted
    offID = offering.fullId #URL format of full id

    if foi != None:  # check if feature of interest should be declare
        featureID = 'http://www.geosmartcity.nl/test/featureOfInterest/' + str(foi.fid) # URL format
        cordX = foi.x	# longitude degrees, float
        cordY =	foi.y	# latitude degrees, float
        height = foi.z		# altitude in meters, float
        h_unit = foi.Hunit # units for horizontal coordinates
        z_unit = foi.Vunit # units for altitude
    else:
        pass

    op_name = procedure.name
    ObsProp = procedure.defn  # URL,
    obs_types= []
    output_list = '' # output list element for describe procedure
    properties_list = []
    for a in sensor_type.pattern["attributes"]:
        ObsPropName = '\"' + a[0] + '\"'  # attribute name
        # print(ObsPropName)
        unit_name = sensor_type.om_types[a[1]]  # om type
        magnitud = a # ??

        obs_name = ObsPropName.replace('\"', '')
        obs_name = "".join(obs_name.split())# observable property name
        output =  '<sml:output name=' + ObsPropName + '><swe:Quantity definition=' + '\"' + (procedure.url + obs_name )+ '\"' + '></swe:Quantity></sml:output>'
        output_list = output_list + output
        properties_list.append(procedure.url + obs_name)  # add property identifier to the list.
        #  prepare list of measurement types
        # A sensor can not registry duplicated sensor types.
        this_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/"+unit_name
        if this_type not in obs_types:  # when new type appears
            obs_types.append(this_type)
        else:
            continue


    # Unit of measurement:
    unit_name = '\"' + procedure.name + '\"' # double quoted string
    # unit = omType # one of the MO measurement types

    body = {
            "request" : "InsertSensor",
            "service" : "SOS",
            "version" : "2.0.0",
            "procedureDescriptionFormat" : "http://www.opengis.net/sensorML/1.0.1",
            "procedureDescription" : '<sml:SensorML xmlns:swes=\"http://www.opengis.net/swes/2.0\" xmlns:sos=\"http://www.opengis.net/sos/2.0\" xmlns:swe=\"http://www.opengis.net/swe/1.0.1\" xmlns:sml=\"http://www.opengis.net/sensorML/1.0.1\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"1.0.1\"><sml:member>' + '<sml:System><sml:identification><sml:IdentifierList><sml:identifier name=\"uniqueID\"><sml:Term definition=\"urn:ogc:def:identifier:OGC:1.0:uniqueID\"><sml:value>' + procedureID + '</sml:value></sml:Term></sml:identifier><sml:identifier name=\"longName\"><sml:Term definition=\"urn:ogc:def:identifier:OGC:1.0:longName\"><sml:value>' + longName + '</sml:value></sml:Term></sml:identifier><sml:identifier name=\"shortName\"><sml:Term definition=\"urn:ogc:def:identifier:OGC:1.0:shortName\"><sml:value>' + shortName + '</sml:value></sml:Term></sml:identifier></sml:IdentifierList></sml:identification><sml:capabilities name=\"offerings\"><swe:SimpleDataRecord><swe:field name=' + offName + '><swe:Text definition=\"urn:ogc:def:identifier:OGC:offeringID\"><swe:value>'+ offID + '</swe:value></swe:Text></swe:field></swe:SimpleDataRecord></sml:capabilities><sml:capabilities name=\"featuresOfInterest\"><swe:SimpleDataRecord><swe:field name=\"featureOfInterestID\"><swe:Text><swe:value>'+ featureID + '</swe:value></swe:Text></swe:field></swe:SimpleDataRecord></sml:capabilities><sml:position name=\"sensorPosition\"><swe:Position referenceFrame=\"urn:ogc:def:crs:EPSG::4326\"><swe:location><swe:Vector gml:id=\"STATION_LOCATION\"><swe:coordinate name=\"easting\"><swe:Quantity axisID=\"x\"><swe:uom code=\"degree\"/><swe:value>'+ str(cordX) + '</swe:value></swe:Quantity></swe:coordinate><swe:coordinate name=\"northing\"><swe:Quantity axisID=\"y\"><swe:uom code=' + '\"' + h_unit + '\"' + ' /><swe:value>' + str(cordY) + '</swe:value></swe:Quantity></swe:coordinate><swe:coordinate name=\"altitude\"><swe:Quantity axisID=\"z\"><swe:uom code=' + '\"' + z_unit + '\"' + '/><swe:value>' + str(height) + '</swe:value></swe:Quantity></swe:coordinate></swe:Vector></swe:location></swe:Position></sml:position><sml:inputs><sml:InputList><sml:input name=' + '\"' + op_name + '\"' + '><swe:ObservableProperty definition=' + '\"' + ObsProp + '\"' + '/></sml:input></sml:InputList></sml:inputs><sml:outputs><sml:OutputList>' +
                                     output_list + '</sml:OutputList></sml:outputs></sml:System></sml:member></sml:SensorML>',
            "observableProperty": properties_list, #
            "observationType": obs_types,
            "featureOfInterestType" : "http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint"}

    return body


def insertObservation(observation, foi, to_offering, with_procedure, observed_property=str, geom=True):
    '''
    Prepares the body of InsertObservation request using JSON binding
    :param Offering: pre-existing offering in the OSO
    :param to_procedure: existing procedure for the observation
    :param observation: observation object
    :param observed_property: property to which this observation belongs to
    :return: body for a insert observatio request in JSON
    '''

    # Observation offering
    # a URI to the offering description
    offId = to_offering.id  # of any type
    offering = "http://www.geosmartcity.nl/test/offering/" + str(offId)

    # Observation ID
    obsId = observation.id # of any type
    observationId = "http://www.geosmartcity.nl/test/observation/" + str(obsId)

    # Selected measurement type.
    omtype = observation.uom
    mtype = "http://www.opengis.net/def/observationType/OGC-OM/2.0/" + omtype

    # Procedure
    # The procedure should be previously declared in 'InsertSensor'
    # An URI to the procedure description. Any type of URL
    proc =  "http://www.geosmartcity.nl/test/procedure/" + str(with_procedure.pid)
    # declared in procedure description
    # One of any type of URI
    observedProp = "http://www.geosmartcity.nl/test/observableProperty/" + "".join(observed_property.split())

    # ID for Feature of Interest for current observation
    # Of any type URI
    featureID = "http://www.geosmartcity.nl/test/featureOfInterest/" + str(foi.fid)

    featureName= "Name for " + str(foi.fid) # string identifying the name of the feature of interest

    # Geometry type for the feature of interest. Most of the time 'Point'.
    featureType = 'Point' # one of any of the types from the Simple Access Feature Model

    # geometry property of feature of interest.
    featureCord = [foi.y, foi.x]  # Latitude, Longitude := (Y, X)

    #Phenomenon, Declare unit of measurements and values.
    #  Elements should be in accordance with the MO type.
    # For most real time observations PhenomenonTime and resultTime are (practically) the same.

    phenomenonTime = observation.phTime # Time at which observation started
    resultTime = observation.rTime # Time at which result of observation was generates

    # Unit of measurement for observation value, when declaring a OM_Measurement
    mag = observation.unit
    # for category observation
    codespace = 'codespace'

    # Observed value for the declared time
    # Should be of proper data type for OM type
    phenomenonValue = observation.Value #example for OM_Measurement

    result = ''

    # More conditions are need for other OM types
    if omtype == "OM_Measurement":
        result = {"uom": mag,
                  "value": phenomenonValue}
    elif omtype == "OM_CategoryObservation":
        result = {"codespace": codespace,
                  "value": phenomenonValue}
    elif omtype == "OM_GeometryObservation":
        result = {"type": "Point",
                        "coordinates": [foi.x, foi.y]}

    else: # CountObservation, TruthObservation, TextObservation
        result = phenomenonValue

    if geom is True:
        #  prepare body including feature of interest declaration
        body = {
            "request": "InsertObservation",
            "service": "SOS",
            "version": "2.0.0",
            "offering": offering,
            "observation": { # optionally a list of observation.
                "identifier": {
                    "value": observationId,
                    "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                },
                "type": mtype,
                "procedure": proc,
                "observedProperty": observedProp, #One defined in procedure description
                "featureOfInterest": {
                    "identifier": {
                        "value": featureID,
                        "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                    },
                    "name": [
                        {
                            "value": featureName,
                            "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                        }
                    ],
                    "sampledFeature": [
                        "http://www.52north.org/test/featureOfInterest/world"
                    ],
                    "geometry": {
                        "type": featureType,
                        "coordinates": featureCord ,
                        "crs": {
                            "type": "name",
                            "properties": {
                                "name": "EPSG:4326"
                            }
                        }
                    }
                },
                "phenomenonTime": phenomenonTime,
                "resultTime": resultTime,
                # Result elements depend of type of MO
                "result":
                    result
            }
        }

    else:
        #  prepare body WITHOUT feature of interest declaration
        body = {
            "request": "InsertObservation",
            "service": "SOS",
            "version": "2.0.0",
            "offering": offering,
            "observation": {  # optionally a list of observation.
                "identifier": {
                    "value": observationId,
                    "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                },
                "type": mtype,
                "procedure": proc,
                "observedProperty": observedProp,  # One defined in procedure description
                "featureOfInterest": featureID,
                "phenomenonTime": phenomenonTime,
                "resultTime": resultTime,
                # Result elements depend of type of MO
                "result":
                    result
            }
        }

    # Return dictionary with InsertObservation  elements
    return body


def insertObservationSP(observation, foi, to_offering, with_procedure, observed_property=str):
    '''
    Prepares the body of InsertObservation request using  the Spatil Profile for JSON binding.
    Every observation must have a geometry
    :param to_offering: pre-existing offering in the SOS
    :param foi: contains location of sensor
    :param with_procedure: existing procedure for the observation
    :param observation: observation object
    :param observed_property: property to which this observation belongs to
    :return: body for a insert observation with spatial profile
    '''

    # Observation offering
    # a URI to the offering description
    offId = to_offering.id  # of any type
    offering = "http://www.geosmartcity.nl/test/offering/" + str(offId)

    # Observation ID
    obsId = observation.id # of any type
    observationId = "http://www.geosmartcity.nl/test/observation/" + str(obsId)

    # Selected measurement type.
    omtype = observation.uom
    mtype = "http://www.opengis.net/def/observationType/OGC-OM/2.0/" + omtype

    # Procedure
    # The procedure should be previously declared in 'InsertSensor'
    # An URI to the procedure description. Any type of URL
    proc =  "http://www.geosmartcity.nl/test/procedure/" + str(with_procedure.pid)
    # declared in procedure description
    # One of any type of URI
    observedProp = "http://www.geosmartcity.nl/test/observableProperty/" + "".join(observed_property.split())

    # Geometry type for the feature of interest. Most of the time 'Point'.
    featureType = 'Point'  # one of any of the types from the Simple Access Feature Model

    # ID for Feature of Interest for current observation
    # Of any type URI
    # Removed in the Spatial profile to allow auto-generations
    featureID = "http://www.geosmartcity.nl/test/featureOfInterest/" + str(foi.fid)

    featureName= "Name for " + str(foi.fid) # string identifying the name of the feature of interest

    # Sampling Geometry
    featureCord = [foi.y, foi.x]  # Latitude, Longitude := (Y, X)

    #Phenomenon, Declare unit of measurements and values.
    #  Elements should be in accordance with the MO type.
    # For most real time observations PhenomenonTime and resultTime are (practically) the same.

    phenomenonTime = observation.phTime # Time at which observation started
    resultTime = observation.rTime # Time at which result of observation was generates

    # Unit of measurement for observation value, when declaring a OM_Measurement
    mag = observation.unit
    # for category observation
    codespace = 'codespace'

    # Observed value for the declared time
    # Should be of proper data type for OM type
    phenomenonValue = observation.Value #example for OM_Measurement

    result = ''

    # More conditions are need for other OM types
    if omtype == "OM_Measurement":
        result = {"uom": mag,
                  "value": phenomenonValue}
    elif omtype == "OM_CategoryObservation":
        result = {"codespace": codespace,
                  "value": phenomenonValue}
    elif omtype == "OM_GeometryObservation":
        result = {"type": "Point",
                        "coordinates": [foi.x, foi.y]}

    else: # CountObservation, TruthObservation, TextObservation
        result = phenomenonValue


    #  prepare body with mandatory geometry 'parameter'
    body = {
        "request": "InsertObservation",
        "service": "SOS",
        "version": "2.0.0",
        "offering": offering,
        "observation": {  # optionally a list of observation.
            "identifier": {
                "value": observationId,
                "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
            },
            "type": mtype,
            "procedure": proc,

            # Extra parameter reporting current location
            "parameter": {
                "NamedValue": {
                    "name": "http://www.opengis.net/def/param-name/OGC-OM/2.0/samplingGeometry",
                    "value": {
                        "type": featureType,
                        "coordinates": featureCord
                    }
                }
            },
            "observedProperty": observedProp,  # One defined in procedure description
            "featureOfInterest": {
                "identifier": {
                    "value": featureID,
                    "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                },
                "name": [
                    {
                        "value": featureName,
                        "codespace": "http://www.opengis.net/def/nil/OGC/0/unknown"
                    }
                ],
                "sampledFeature": [
                    "http://www.52north.org/test/featureOfInterest/world"
                ],
                "geometry": {
                    "type": featureType,
                    "coordinates": featureCord,
                    "crs": {
                        "type": "name",
                        "properties": {
                            "name": "EPSG:4326"
                        }
                    }
                }
            },
            "phenomenonTime": phenomenonTime,
            "resultTime": resultTime,
            # Result elements depend of type of MO
            "result":
                result
        }
    }
    # Return dictionary with InsertObservation  elements
    return body


def insertMobileSensor(offering, procedure,  foi, sensor_type):
    """
    Prepares the body of a InsertSensor request to register a mobile sensor. Based on SensorML 2.0.
    Allowed sensor types: insitu-fixed, insitu-mobile.
    :param offering: an instance of class Offering.Type object.
    :param procedure: instance of class Procedure. type object.
    :param foi: feature of interest. Instance of FoI
    :param sensor_type: SensorType object
    :return: valid body for an InsertSensor request.
    """
    # TODO: extend to chose among all sensor types, or make type a parameter
    #  for JSON double quotes: \", or \u0022
    #  specify procedure ID:
    procedureID = 'http://www.geosmartcity.nl/test/procedure/' + str(procedure.pid) # URL format
    shortName = 'short name' #string
    longName = 'long name' #string

    # Offering values
    offName = offering.name #Offering name, double quoted
    offID = offering.fullId #URL format of full id

    if foi != None:  # check if feature of interest should be declare
        featureID = 'http://www.geosmartcity.nl/test/featureOfInterest/' + str(foi.fid) # URL format
        cordX = foi.x	# longitude degrees, float
        cordY =	foi.y	# latitude degrees, float
        height = foi.z		# altitude in meters, float
        h_unit = foi.Hunit # units for horizontal coordinates
        z_unit = foi.Vunit # units for altitude
    else:
        pass


    op_name = procedure.name
    ObsProp = procedure.defn  # URL,
    # print(ObsProp)
    obs_types= []
    output_list = '' # output list element for describe procedure
    properties_list = []
    for a in sensor_type.pattern["attributes"]:
        ObsPropName = '\"' + a[0] + '\"'  # attribute name
        # print(ObsPropName)
        unit_name = sensor_type.om_types[a[1]]  # om type

        obs_name = ObsPropName.replace('\"', '')
        obs_name = "".join(obs_name.split())# observable property name
        #  TODO: check complience of <Output> types in procedure definition and OM_types in sensorML
        output =  '<sml:output name=' + "".join(ObsPropName.split()) + '><swe:Quantity definition=' + '\"' + (procedure.url + obs_name ) + '\"' + '> <swe:uom code=\"'+ unit_name + '\"/></swe:Quantity></sml:output>'
        output_list = output_list + output
        properties_list.append(procedure.url + obs_name)  # add property identifier to the list.
        #  prepare list of measurement types
        # A sensor can not registry duplicated sensor types.
        this_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/"+ unit_name
        if this_type not in obs_types:  # when new type appears
            obs_types.append(this_type)
        else:
            continue


    # Unit of measurement:
    unit_name = '\"' + procedure.name + '\"' # double quoted string
    # unit = omType # one of the MO measurement types

    # Output name=, does not accept white spaces.
    # Identifier for first procedure works as parent, it cannot be the same as the new procedure.
    # AttachedTo parameter is not compulsory. # TODO: procedure description should modified to match a desired data model.
    body = {
            "request" : "InsertSensor",
            "service" : "SOS",
            "version" : "2.0.0",
            "procedureDescriptionFormat" : "http://www.opengis.net/sensorml/2.0",
            "procedureDescription": '<sml:PhysicalSystem gml:id=\"sensor9\" xmlns:swes=\"http://www.opengis.net/swes/2.0\" xmlns:sos=\"http://www.opengis.net/sos/2.0\" xmlns:swe=\"http://www.opengis.net/swe/2.0\" ' + 'xmlns:sml=\"http://www.opengis.net/sensorml/2.0\" xmlns:gml=\"http://www.opengis.net/gml/3.2\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:gco=\"http://www.isotc211.org/2005/gco\" xmlns:gmd=\"http://www.isotc211.org/2005/gmd\"><gml:identifier codeSpace=\"uniqueID\">' + procedureID + '</gml:identifier><sml:identification><sml:IdentifierList><sml:identifier><sml:Term definition=\"urn:ogc:def:identifier:OGC:1.0:longName\"><sml:label>longName</sml:label><sml:value>' + longName + '</sml:value></sml:Term></sml:identifier><sml:identifier><sml:Term definition=\"urn:ogc:def:identifier:OGC:1.0:shortName\"><sml:label>shortName</sml:label><sml:value>' + shortName + '</sml:value></sml:Term></sml:identifier></sml:IdentifierList></sml:identification><sml:capabilities name=\"offerings\"><sml:CapabilityList><sml:capability name=\"offeringID\"><swe:Text definition=\"urn:ogc:def:identifier:OGC:offeringID\"><swe:label>offeringID</swe:label><swe:value>' + offID + '</swe:value></swe:Text></sml:capability></sml:CapabilityList></sml:capabilities>' + '<sml:capabilities name=\"metadata\"><sml:CapabilityList><sml:capability name=\"insitu\"><swe:Boolean definition=\"insitu\"><swe:value>true</swe:value></swe:Boolean> </sml:capability><sml:capability name=\"mobile\"><swe:Boolean definition=\"mobile\"><swe:value>true</swe:value></swe:Boolean></sml:capability></sml:CapabilityList></sml:capabilities>' + '<sml:featuresOfInterest><sml:FeatureList definition=\"http://www.opengis.net/def/featureOfInterest/identifier\"><swe:label>featuresOfInterest</swe:label><sml:feature xlink:href=\"' + featureID + '\"/></sml:FeatureList></sml:featuresOfInterest><sml:inputs><sml:InputList><sml:input name=' + '\"' + op_name + '\"' + '><sml:ObservableProperty definition=' + '\"' + ObsProp + '\"' + ' /></sml:input></sml:InputList></sml:inputs><sml:outputs><sml:OutputList>' + output_list + '</sml:OutputList></sml:outputs><sml:position><swe:Vector referenceFrame=\"urn:ogc:def:crs:EPSG::4326\"><swe:coordinate name=\"easting\"><swe:Quantity axisID=\"x\"><swe:uom code=' + '\"'+ h_unit + '\"' + '/><swe:value>' +
            str(cordX) + '</swe:value></swe:Quantity></swe:coordinate><swe:coordinate name=\"northing\"><swe:Quantity axisID=\"y\"><swe:uom code=\"' + h_unit + '\" /><swe:value>' +
            str(cordY) + '</swe:value></swe:Quantity></swe:coordinate><swe:coordinate name=\"altitude\"><swe:Quantity axisID=\"z\"><swe:uom code=\"' + z_unit + '\" /><swe:value>' + str(
            height) + '</swe:value></swe:Quantity></swe:coordinate></swe:Vector></sml:position></sml:PhysicalSystem>',
        "observableProperty":  properties_list, #
        "observationType": obs_types,
        "featureOfInterestType": "http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint"}

    return body



def insertComplexObservation():
    """

    :return:
    """
    # To use conplex observation:
    #   1. Register sensor with with obsevation type 'OM_ComplexObservation'
    #   2. Insert observation using the Complex observation request body.


    return


def deleteSensor(procedure_id=str):  # TODO: Need to be completed


    '''
    Prepares the body of a delete sensor request for the JSON binding.
    :param procedure_id_id: Identifier of the procedure URI.
    '''

    body = {
        "request": "DeleteSensor",
        "service": "SOS",
        "version": "2.0.0",
        "procedure": procedure_id
    }
    return body


def deleteObservation(observation_uri):  # TODO: To be completed
    '''
    :param observation_uri: URL identifying an observation.
    '''

def main():

    import wrapper
    import data_loader

    dir = 'c:/sos_santander/raw_data/sample/'
    f_name = "santander_example_data.json"
    f_name2 = "data_stream-2016-07-01T080007.json"
    h_dir = 'c:/Temp/hist_temp/'

    url = 'http://130.89.217.201:8080/sos-4.4/service'
    token = 'TWFudWVsIEdhcmNpYQ=='

    f = wrapper.FoI('m', 'm', (1,2,3), 'my/feature/')
    tsensor = wrapper.SensorType('light')
    off = wrapper.Offering('my/domain/', '1', 'offering1')
    proc = wrapper.Procedure('1', 'procedure1', 'my/procedure', 'lux')

    r = insertMobileSensor(off, proc, f, tsensor)
    print(r)

    # ms =wrapper.sosPost(r, url, token)





if __name__ == '__main__':
    main()