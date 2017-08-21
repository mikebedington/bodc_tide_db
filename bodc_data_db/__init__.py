import numpy as np
import sqlite3 as sq
import datetime as dt
import subprocess as sp
import gpxpy.geo as gpg

SQL_UNIX_EPOCH = dt.datetime(1970,1,1,0,0,0)

class db_tide():
	def __init__(self, db_name):
		if db_name[-3:] != '.db':
			db_name += '.db'		
		self.conn = sq.connect(db_name)
		self.create_table_sql = {}
		self.retrieve_data_sql = {}
		self.c = self.conn.cursor()

	def execute_sql(self, sql_str):
		self.c.execute(sql_str)
		return self.c.fetchall()

	def make_create_table_sql(self, table_name, col_list):
		create_str = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('
		for this_col in col_list:
			create_str += this_col
			create_str += ', '
		create_str = create_str[0:-2]
		create_str += ');'
		self.create_table_sql['create_' + table_name] = create_str

	def make_bodc_tables(self):
		add_sql_strings(self)
		for this_table, this_str in self.create_table_sql.items():
			self.execute_sql(this_str)
		error_data = [(0, '', 'No error'), (1, 'M', 'Improbable value flagged by QC'),
						(2, 'N', 'Null Value'), (3, 'T', 'Value interpolated from adjacent values')]
		self.insert_into_table('error_flags', error_data)

	def insert_tide_file(self, file_list):
		for this_file in file_list:
			print('Inserting data from file: ' + this_file)
			this_file_obj = bodc_annual_tide_file(this_file)
			try:
				site_id = self.select_qry('sites', "site_tla == '" + this_file_obj.site_tla + "'", 'site_id')[0][0]
			except:
				try:
					current_id_max = np.max(self.select_qry('sites', None, 'site_id')) 
					site_id = int(current_id_max + 1)
				except:
					site_id = 1

				site_data = [(site_id, this_file_obj.site_tla, this_file_obj.site_name, this_file_obj.lon, this_file_obj.lat, '')]
				self.debug_data = site_data
				self.insert_into_table('sites', site_data)
	
			site_id_list = [site_id] * len(this_file_obj.seconds_from_ref)
			table_data = list(zip(site_id_list, this_file_obj.seconds_from_ref, this_file_obj.elevation_data,
							this_file_obj.elevation_flag, this_file_obj.residual_data, this_file_obj.residual_flag))
			self.insert_into_table('gauge_obs', table_data) 

	def insert_into_table(self, table_name, data):
		no_rows = len(data)
		no_cols = len(data[0])
		qs_string = '('
		for this_x in range(no_cols):
			qs_string += '?,'
		qs_string = qs_string[:-1]
		qs_string += ')'
	
		if no_rows > 1:
			self.c.executemany('insert into ' + table_name + ' values ' + qs_string, data)
		elif no_rows == 1:
			self.c.execute('insert into ' + table_name + ' values ' + qs_string, data[0])
		self.conn.commit()

	def select_qry(self, table_name, where_str, select_str = '*', order_by_str = None, inner_join_str = None):
		qry_string = 'select ' + select_str + ' from ' + table_name
		if inner_join_str is not None:
			qry_string += ' inner join ' + inner_join_str
		if where_str is not None:
			qry_string += ' where ' + where_str	
		if order_by_str is not None:
			qry_string += ' order by ' + order_by_str
		return self.execute_sql(qry_string)

	def get_tidal_series(self, station_identifier, start_date_dt=None, end_date_dt=None):
		select_str = "time_int, elevation, elevation_flag"
		table_name = "gauge_obs as go"
		inner_join_str = "sites as st on st.site_id = go.site_id"
		
		if isinstance(station_identifier, str):
			where_str = "st.site_tla = '" + station_identifier + "'"
		else:
			where_str = "st.site_id = " + str(int(station_identifier))

		if start_date_dt is not None:
			start_sec = dt_to_epochsec(start_date_dt)
			where_str += " and go.time_int >= " + str(start_sec)
		if end_date_dt is not None:
			end_sec = dt_to_epochsec(end_date_dt)
			where_str += " and go.time_int <= " + str(end_sec)
		order_by_str = 'go.time_int'	
		return_data = self.select_qry(table_name, where_str, select_str, order_by_str, inner_join_str)
		if not return_data:
			print('No data available')
		else:
			return_data = np.asarray(return_data)
			date_list = [epochsec_to_dt(this_time) for this_time in return_data[:,0]]
			return np.asarray(date_list), return_data[:,1:]

	def get_nearest_gauge_id(self, lat, lon):
		sites_lat_lon = np.asarray(self.select_qry('sites', None, 'site_id, lat, lon'))
		min_dist = 9999999999999
		closest_gauge_id = -999
		for this_row in sites_lat_lon:
			this_dist = gpg.haversine_distance(lat, lon, this_row[1], this_row[2])
			if this_dist < min_dist:
				min_dist = this_dist
				closest_gauge_id = this_row[0]
		return int(closest_gauge_id), min_dist

	def close_conn(self):
		self.conn.close()
	


class bodc_annual_tide_file():
	def __init__(self, file_name, header_length = 11):
		'''
		Assumptions: file name of the form yearTLA.txt,     , 

		'''
		clean_tide_file(file_name, header_length)
		with open(file_name) as f:
			header_lines= [next(f) for this_line in range(header_length)]	

		for this_line in header_lines:
			if 'ongitude' in this_line:
				self.lon = [float(s) for s in this_line.split() if is_number(s)][0]
			if 'atitude' in this_line:			
				self.lat = [float(s) for s in this_line.split() if is_number(s)][0]
			if 'Site' in this_line:
				site_str_raw = this_line.split()[1:]
				if len(site_str_raw) == 1:
					site_str = site_str_raw[0]
				else:
					site_str = ''			
					for this_str in site_str_raw:
						site_str += this_str				

		self.site_name = site_str
		self.site_tla = file_name.split('/')[-1][4:7]			

		raw_data = np.loadtxt(file_name, skiprows=header_length, dtype=bytes).astype(str)

		seconds_from_ref = []
		for this_row in raw_data:
			this_dt_str = this_row[1] + ' ' + this_row[2]
			this_seconds_from_ref = dt_to_epochsec(dt.datetime.strptime(this_dt_str, '%Y/%m/%d %H:%M:%S'))
			seconds_from_ref.append(int(this_seconds_from_ref))	
		self.seconds_from_ref = seconds_from_ref

		elevation_data = []
		elevation_flag = []
		residual_data = []
		residual_flag = []
		for this_row in raw_data:
			meas, error_code = parse_tide_obs(this_row[3])
			elevation_data.append(meas)
			elevation_flag.append(error_code)
			meas, error_code = parse_tide_obs(this_row[4])
			residual_data.append(meas)
			residual_flag.append(error_code)
		self.elevation_data = elevation_data
		self.elevation_flag = elevation_flag
		self.residual_data = residual_data
		self.residual_flag = residual_flag


def parse_tide_obs(in_str):
	error_code_dict = {'M':1, 'N':2, 'T':3}
	try: 
		int(in_str[-1])
		error_code = 0
		meas = float(in_str)
	except:
		error_code_str = in_str[-1]
		meas = float(in_str[0:-1])
		try:
			error_code = error_code_dict[error_code_str]
		except:
			print('Unrecognised error code')
			return
	return meas, error_code

def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		return False
	
def clean_tide_file(file_name, header_length):
	sed_str = "sed -i.bak '"+ str(header_length + 1) + ",$ {/^ [0-9]/!d}' " + file_name
	sp.call([sed_str], shell=True)	

def dt_to_epochsec(time_to_convert):
	return (time_to_convert - SQL_UNIX_EPOCH).total_seconds()

def epochsec_to_dt(time_to_convert):
    return SQL_UNIX_EPOCH + dt.timedelta(seconds=time_to_convert)


def add_sql_strings(db_obj):	
	bodc_tables = {'gauge_obs':['site_id integer NOT NULL', 'time_int integer NOT NULL', 
								'elevation real NOT NULL', 'elevation_flag integer', 'residual real', 'residual_flag integer',
								'PRIMARY KEY (site_id, time_int)', 'FOREIGN KEY (site_id) REFERENCES sites(site_id)',
								'FOREIGN KEY (elevation_flag) REFERENCES error_flags(flag_id)',
								'FOREIGN KEY (residual_flag) REFERENCES error_flags(flag_id)'],
					'sites':['site_id integer NOT NULL', 'site_tla text NOT NULL', 'site_name text', 'lon real', 'lat real',
								'other_stuff text', 'PRIMARY KEY (site_id)'],
					'error_flags':['flag_id integer NOT NULL', 'flag_code text', 'flag_description text']}

	for this_key, this_val in bodc_tables.items():
		db_obj.make_create_table_sql(this_key, this_val)

	

