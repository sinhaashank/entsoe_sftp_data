import pandas as pd
from datetime import datetime, timezone
import logging
import argparse

logger = logging.getLogger(__name__)
#logger = logging.getLogger('logger_name')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)

###############################################################################################
def argument_parser():
    from arghelper import is_valid_file, is_valid_directory
    
    # Instantiate the parser
    parser = argparse.ArgumentParser(description=(''))
    # Add Arguments
    parser.add_argument('--config_file_entsoe', dest='config_file_entsoe', action="store", 
                        required=True, type=is_valid_file,
                        default='config_entsoe.ini',
                        help='Config file ENTSOE')
    parser.add_argument('--config_file_entsoe_data_types', dest='config_file_entsoe_data_types', action="store", 
                        required=True, type=is_valid_file,
                        default='config_entsoe_last_update.ini',
                        help='Config file Last Update')
    parser.add_argument('--file_mapping_countries_bidding_zones', dest='file_mapping_countries_bidding_zones', 
                        action="store", 
                        required=True, type=is_valid_file,
                        help='Mapping file Coutries to BBidding Zones')
    parser.add_argument('-l','--logfile',
                        dest='logfile', action="store", required=True, #type=is_valid_file,
                        help='Log file')
    return parser.parse_args()

###########################################################################################
        
list_generation_types = [
    'Biomass',
    'Fossil Brown coal/Lignite', # generation DE
    'Fossil Coal-derived gas', # installed_generation_capacity_DE
    'Fossil Gas',
    'Fossil Hard coal',
    'Fossil Oil',
    'Geothermal', # generation DE
    'Hydro Pumped Storage',
    'Hydro Run-of-river and poundage',
    'Hydro Water Reservoir', 
    'Marine', 
    'Nuclear', 
    'Other',
    'Other renewable', # generation DE
    'Solar',
    'Waste',
    'Wind Offshore', 
    'Wind Onshore',
]

############################################################################################
def sync_data(
    hostname = None,
    username = None,
    password = None,
    remote_dir = None,
    local_dir = None,
    list_data_types = None, 
    
    ### Changed 
    
    #list_data_types = [
    #    #'DayAheadPrices',
    #    'DayAheadPrices_12.1.D',
    #    #'AggregatedGenerationPerType',
    #    'AggregatedGenerationPerType_16.1.B_C'
    #],
    
):
    import os
    from sftp_utils import sftp_sync_dirs
    
    dict_modified_files = dict()
    
    for data_type in list_data_types:
        logger.info("SFTP: Synchronizing data for data type: '{}'...".format(data_type))
        list_modified_files = sftp_sync_dirs(
            hostname = hostname,
            username = username,
            password = password,
            remote_dir = os.path.join(remote_dir, data_type),
            local_dir = os.path.join(local_dir, data_type),
        )
        
        dict_modified_files[data_type] = list_modified_files
        
    return dict_modified_files

############################################################################################
def update_entsoe(
    #time_zone = None,
    config_file_entsoe = None,
    config_file_entsoe_data_types = None,
    file_mapping_countries_bidding_zones = None,
    
    #save_data = True,
    #auto_update = False,
):
    import pandas as pd
    from config_utils import entosoe_load_config_file
    
    (
        hostname,
        username,
        password,
        remote_dir,
        local_dir,
        list_data_types_to_sync,
        
        ip_mongo_server, 
        library_name,
        
        list_data_types
    ) = entosoe_load_config_file(
        config_file_entsoe = config_file_entsoe,
        config_file_entsoe_data_types = config_file_entsoe_data_types,
        file_mapping_countries_bidding_zones = file_mapping_countries_bidding_zones,
    )
            
    #-----------------------------------------------------------------------------------
    dict_modified_files = sync_data(
        hostname = hostname,
        username = username,
        password = password,
        remote_dir = remote_dir,
        local_dir = local_dir,
        list_data_types = list_data_types_to_sync,
    )        
    
    #-----------------------------------------------------------------------------------
    for entsoe_data_type in list_data_types:        
        if entsoe_data_type.data_format in [
            'country', 
            'country_group', 
            'flow'
        ]:
            entsoe_data_type.set_months_to_update(dict_modified_files)
            entsoe_data_type.update_data_type()
            
###############################################################################################
if __name__ == '__main__':
            
    args = argument_parser()
    
    config_file_entsoe    = args.config_file_entsoe
    config_file_entsoe_data_types    = args.config_file_entsoe_data_types
    file_mapping_countries_bidding_zones = args.file_mapping_countries_bidding_zones
    logfile = args.logfile
    
    #------------------------------------------------------------------------------------------
    import logging.config
    #FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
    FORMAT = "%(asctime)s [%(filename).5s...:%(lineno)s] %(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, 
                        datefmt='%Y-%m-%d %H:%M:%S', 
                        level=logging.INFO)
    
    # create file handler which logs even debug messages
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    fh.setFormatter(formatter)    
    #
    #global logger
    #logger = logging.getLogger(__name__)
    logger = logging.getLogger('entsoe')
    logger.addHandler(fh)
    #------------------------------------------------------------------------------------------
    logger.info("logging into file " + logfile)
    
    #------------------------------------------------------------------------------------------
    update_entsoe(
        config_file_entsoe = config_file_entsoe,
        config_file_entsoe_data_types = config_file_entsoe_data_types,
        file_mapping_countries_bidding_zones = file_mapping_countries_bidding_zones,
    )
        
    
