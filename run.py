import pandas as pd
import numpy as np
import os
import sys

# load_ext autoreload
import lib_entsoe
import sftp_utils
import mongodb_store_load
import config_utils
import entsoe_data_type
import entsoe_countries
# autoreload 1

# pd.set_option("display.max_columns",201)
# pd.set_option("display.max_colwidth",101)
# pd.set_option("display.max_rows",500)


from arctic import Arctic, CHUNK_STORE

conn = Arctic('localhost')
conn.initialize_library('entsoe', lib_type=CHUNK_STORE)
conn.list_libraries()
lib = conn['entsoe']

df_focus = lib_entsoe.update_entsoe(
    config_file_entsoe = 'config_entsoe.ini',
    config_file_entsoe_data_types = 'config_entsoe_last_update.ini',
    file_mapping_countries_bidding_zones = 'countries_bidding_zones.xlsx',
)