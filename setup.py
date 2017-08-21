from setuptools import setup

setup(name='bodc_data_db',
		version='0.1.0',
		description='Class for working with the BODC tide data and turning it into an sql database', 
		url='https://gitlab.ecosystem-modelling.pml.ac.uk/mbe/bodc_tide_db',
		author='mbe',
		author_email='mbe@pml.ac.uk',
		packages=['bodc_data_db'],
		#install_requires=['numpy', 'sqlite3', 'datetime', 'subprocess', 'gpxpy.geo'],
		zip_safe=False)
