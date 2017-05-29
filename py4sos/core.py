"""
Function for the Core Operation Profile in a SOS.
This set of function retrieve data from a SOS through a RESTful API
Author: Manuel G. Garcia
Created: 24-05-2017
"""

import requests

def send_request(body, url, token):
    """
    Sends a request to a SOS using POST method
    :param body: body of the request formatted as JSON
    :param token: Authorization Token for an existing SOS.
    :param url: URL to the endpoint where the SOS can be accessed
    :return: Server response to response formatted as JSON
    """

    # Add headers:
    headers = {'Authorization': str(token), 'Accept': 'application/json'}
    response = requests.post(url, headers=headers, json=body)

    response.raise_for_status()  # raise HTTP errors

    return response


def getObservationByTime(sos, procedure, offering, property_, feature_of_interest, time_interval):
    """
    Generate a json-formatted body request for a SOS, which retrieves data based on a time interval.
    :param sos: Object describing an existing SOS with valid URL and token.
    :param procedure: procedure identifier as URI
    :param offering: offering identifier as URI
    :param property_: observable property
    :param feature_of_interest: feature of interest identifier as URI
    :param time_interval: [start_time, end_time], iso format with time zone, string
    :return: SOS response containing JSON-formatted Observations filtered by time.
    """
    # TODO: test time interval validity. start_time smaller than end_time

    request_body={
        "request": "GetObservation",
        "service": "SOS",
        "version": "2.0.0",
        "procedure": procedure,
        "offering":  offering,
        "observedProperty": property_,
        "featureOfInterest": feature_of_interest,
        "temporalFilter": {
            "during": {
                "ref": "om:phenomenonTime",
                "value": [
                    time_interval[0],
                    time_interval[1]
                ]
            }
        }
    }

    response = send_request(request_body, sos.sosurl, sos.token)

    return response.json()


def getObservationById(sos, ids):
    """
    Retrives data from an existing SOS using observation IDs.
    :param sos: Object describing an existing SOS with valid URL and token.
    :param ids: a single string or a list of strings  with observations IDs. IDs as URIs
    :return: SOS response containing observations that matches the ID(s), formatted as JSON.
    """

    # check if ids is a list of strings:
    def checktype(obj):
        return bool(obj) and all(isinstance(elem, str) for elem in obj)

    if checktype(ids) is not True:
        print('ERROR: The parameter "id" is not a list of strings')
        print('Check the values for the input parameter')
        return
    else:
        pass

    request_body = {"request": "GetObservationById",
                    "service": "SOS",
                    "version": "2.0.0",
                    "observation": ids
                    }

    response = send_request(request_body, sos.sosurl, sos.token)

    return response.json()


def getCapabilites(sos, level='service'):
    """
    Retrives the capabilites of an existing SOS, formatted as JSON
    :param sos: Object describing an existing SOS with valid URL and token.
    :param level: Level of details in the capabilities of an SOS. Possible values: 'service', 'content', 'operations',
     'all', and 'minimal'.
    :return: Capabilities of an SOS formatted as JSON
    """

    # classified level of detail based by requesting selected sections
    section_levels = {"service": [
        "ServiceIdentification", "ServiceProvider"
        ],
        "content": ["Contents"],
        "operations": ["OperationsMetadata"],
        "all": [
            "ServiceIdentification",
            "ServiceProvider",
            "OperationsMetadata",
            "FilterCapabilities",
            "Contents"
        ]
    }

    # Test for valid values for 'level' parameter:
    valid_levels = ['service', 'content', 'operations', 'all', 'minimal']
    if level in valid_levels:  # if parameter is in valid_levels

        if level != 'minimal':
            request_body = {"request": "GetCapabilities",
                            "service": "SOS",
                            "sections": section_levels[level]
                            }
        else:  # for level 'minimal'
            request_body = {"request": "GetCapabilities",
                            "service": "SOS"
                            }
        response = send_request(request_body, sos.sosurl, sos.token)  # send request
        return response.json()

    else: # When no level input value matches
        print('--->> Error: The value for the "level" parameter is not valid!!')
        print("------>>> Valid values are: 'service', 'content', 'operations', 'all', and 'minimal'")
        return None


def getDataAvailability(sos, procedure, property_, feature_of_interest):
    """
    Requests metadata regarding the availability of data in an existing SOS.
    :param sos: Object describing an existing SOS with valid URL and token.
    :param procedure: procedure identifier as URI
    :param property_: observable property identifier as URI
    :param feature_of_interest:  feature of interest identifier as URI
    :return: availability of data in a SOS filtered by the input parameters, formatted as JSON
    """

    #  TODO: Expand function to the case of multiple filters and no filter

    request_body = {"request": "GetDataAvailability",
                    "service": "SOS",
                    "version": "2.0.0",
                    "procedure": procedure,
                    "observedProperty": property_,
                    "featureOfInterest": feature_of_interest
                    }

    response = send_request(request_body, sos.sosurl, sos.token)  # send request

    return response.json()




def main ():

    import data_loader
    url = 'http://130.89.217.201:8080/sos-4.4/service'
    token = 'TWFudWVsIEdhcmNpYQ=='

    property_ = "http://www.geosmartcity.nl/test/observableProperty/" + 'Luminosity'
    offering = 'http://www.geosmartcity.nl/test/offering/' + 'node217'
    procedure = 'http://www.geosmartcity.nl/test/procedure/' + 'node217'
    foi = 'http://www.geosmartcity.nl/test/featureOfInterest/' + 'node217'
    t_interval = [
        '2016-07-01T00:00:00+01:00',
        '2016-07-01T10:00:00+01:00'
    ]

    ids = ['http://www.geosmartcity.nl/test/observation/node734_Luminosity_1', 'http://www.geosmartcity.nl/test/observation/node734_Luminosity_2']


if __name__ == '__main__':
    main()
