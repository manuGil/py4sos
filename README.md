# py4sos
A python wrapper to handle requests to a Sensor Observation Service (SOS)

This Python package provides a set of functions to handle requests to and forth a Sensor Observation Service (SOS). Its use is restricted to the OGC SOS standard and the implementation provided by 52North. For more information visit: http://52north.org/communities/sensorweb/sos/
This package is currently compatible with the SOS 4.4.x version, and it mainly uses the JSON binding to access the SOS API.

## Adquired Skills

* Python programming
* Parsing JSON files
* OGC SOS standard
* Deploying Java application in Tomcat
* Working with API's


# About the SOS Standard

The SOS Implementation defines operations (function in Python) under tow categories. The Core Profile defines operations to mainly access data already published into a SOS. The Transactional Profile defines operations to register sensor and published data. Some operations are mandatory and some are optional. See http://www.opengeospatial.org/standards/sos for documentation and latest changes.

# About the  52North Implementation

52North currently developes an implementation which complies with the OGC standard. However, it might be that some of the optional operations are not implemented.  Besides, some additional operation have been added to provide more flexibility. The implementation provides an RESTFUl API to access the SOS. Several bindings are available (e.g., XML+SOAP, JSON). For the latest stable version visit: https://github.com/52North/SOS/
