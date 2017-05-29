from setuptools import setup

setup(name='py4sos',
      version='1.0.0',
      description='Python API to the 52North SOS implementation',
      # url='http://github.com/storborg/funniest',
      author='Manuel Garcia',
      author_email='m.g.garcialalvarez@utwente.nl',
      url='gip.itc.nl/resources/magarcia',
      license='MIT',
      packages=['py4sos'],
      install_requires=['requests'],
      classifiers=["Programming Language :: Python","Programming Language :: Python :: 3", "License :: Free for non-commercial use", "Operating System :: Windows", "Development Status :: 2 - Pre-Alpha", "Intended Audience :: Developers","Topic :: Internet :: WWW/HTTP :: HTTP Servers", "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware", "Intended Audience :: Telecommunications Industry", "Topic :: Software Development :: Pre-processors", "Environment :: Web Environment"],
      long_description = """\
      Python API for a Service Observation Service (SOS)
      --------------------------------------------------
      Complies with 
        - 52North SOS Implementation, version 4.4.0

      Core Operations:
        - getCapabilities, getDataAvailability
        - getObservationByID, getObservationByTime
        
      Transactional Operations:
        - insertSensor, insertObservation, insertObservationSP
        
      This version requires Python 3 or later. It requires the 'requests' package.
      """

      )