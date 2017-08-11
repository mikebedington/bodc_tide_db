import os

import bodc_data_db as dbf

data_dir = '/data/euryale4/backup/mbe/Data/BODC_tide_gauge/'
files_list = os.listdir(data_dir)
files_list = [data_dir + this_entry for this_entry in files_list if 'txt' in this_entry and 'txt.bak' not in this_entry]

bodc_db = dbf.db_tide('bodc_tide_gauge')
bodc_db.make_bodc_tables()
bodc_db.insert_tide_file(files_list)
