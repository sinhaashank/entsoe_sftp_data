import pandas as pd
from datetime import datetime, timezone
import logging

#logger = logging.getLogger(__name__)
logger = logging.getLogger('entsoe')
#logger = logging.getLogger('logger_name')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)

###############################################################################################
# Generic Functions
###############################################################################################
def config_get_option(
    file_name = None,
    section = None,
    option = None,
):
    import configparser
    import os
    import sys

    if not os.path.exists(file_name):
        logger.error('Cannot find configuration file %s' % (file_name))
    
    logger.info('Reading configuration file {}'.format(file_name))
    parser = configparser.SafeConfigParser()
    
    # make the parser case sensitive (by default case insensitive)
    parser.optionxform = str
    
    try:
        parser.read(file_name)
        option_value = parser.get(section, option)
        return option_value
    
    except IOError as err:
        logger.error('Problem opening configuration file %s. %s' % (file_name, err))
        return -1
    except configparser.NoOptionError as err:
        logger.error('Problem with configuration file %s. %s' % (file_name, err))
        return -1
    except configparser.NoSectionError as err:
        logger.error('Problem with configuration file %s. %s' % (file_name, err))
        return -1
    except:
        logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        logger.info('Problem with configuration file %s.' % (file_name))
        return -1

###############################################################################################
def config_set_option(
    file_name = None,
    section = None,
    option = None,
    option_value = None,
):
    import configparser
    import os
    import sys

    if not os.path.exists(file_name):
        logger.error('Cannot find configuration file %s' % (file_name))
    
    parser = configparser.SafeConfigParser()
    
    # make the parser case sensitive (by default case insensitive)
    parser.optionxform = str
    
    try:
        logger.info(
            'Updating configuration file {}, ({},{}): {}'
            .format(file_name, section, option, option_value)
        )
        parser.read(file_name)
        parser.set(section, option, str(option_value))
        
        config_file = open(file_name, "w")
        parser.write(config_file)
        config_file.close() 
    
    except IOError as err:
        logger.error('Problem opening configuration file %s. %s' % (file_name, err))
        return -1
    except configparser.NoSectionError as err:
        logger.error('Problem with configuration file %s. %s' % (file_name, err))
        return -1
    except:
        logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        logger.info('Problem with configuration file %s.' % (file_name))
        return -1

###############################################################################################
def config_get_option_list(
    file_name = None,
    section = None,
    option = None,
    sep = None,
):
    value = config_get_option(
        file_name = file_name,
        section = section,
        option = option,
    )
    
    return list(filter(None, (x.strip() for x in value.split(sep))))

###############################################################################################
def config_get_option_list_of_lists(
    file_name = None,
    section = None,
    option = None,
    sep1 = None,
    sep2 = None,
):
    list_value = config_get_option_list(
        file_name = file_name,
        section = section,
        option = option,
        sep = sep1,
    )
        
    return list([x.strip() for x in l.split(sep2)] for l in list_value)

###############################################################################################
# Custom Functions
###############################################################################################
def entosoe_load_config_file(
    config_file_entsoe = None,
    config_file_entsoe_data_types = None,
    file_mapping_countries_bidding_zones = None,
):
    """
    Read config file containing information about:
    - Mongo DB connection
    - ...
    
    """
    import configparser
    import os
    import sys
    from mongodb_store_load import initialize_library
    from entsoe_data_type import EntsoeDataType, EntsoeDataTypeCountry
    from entsoe_data_type import EntsoeDataTypeCountryGroup, EntsoeDataTypeFlow

    logger = logging.getLogger(__name__)
    
    if not os.path.exists(config_file_entsoe):
        logger.error('Cannot find configuration file %s' % (config_file_entsoe))
    
    logger.info('Reading configuration file {}'.format(config_file_entsoe))
    parser = configparser.SafeConfigParser()
    
    # make the parser case sensitive (by default case insensitive)
    parser.optionxform = str
    
    #try:
    if True:    
        # read config file
        parser.read(config_file_entsoe)
        
        hostname = parser.get('sftp_entsoe', 'hostname')
        username = parser.get('sftp_entsoe', 'username')
        password = parser.get('sftp_entsoe', 'password')
        remote_dir = parser.get('sftp_entsoe', 'remote_dir')
        local_dir = parser.get('sftp_entsoe', 'local_dir')
        
        file_coutries_bidding_zones = parser.get('entsoe_coutries_bidding_zones', 'mapping_file')
        column_Country = parser.get('entsoe_coutries_bidding_zones', 'column_Country')
        column_CountryCode = parser.get('entsoe_coutries_bidding_zones', 'column_CountryCode')
        column_change_date = parser.get('entsoe_coutries_bidding_zones', 'column_change_date')
        column_AreaName = parser.get('entsoe_coutries_bidding_zones', 'column_AreaName')
        column_MapCode = parser.get('entsoe_coutries_bidding_zones', 'column_MapCode')
        
        dict_columns_coutries_bidding_zones = {
            'column_Country':  column_Country,
            'column_CountryCode':  column_CountryCode,
            'column_change_date':  column_change_date,
            'column_AreaName':  column_AreaName,
            'column_MapCode':  column_MapCode,
        }
        
        df_coutries_bidding_zones = pd.read_excel(file_mapping_countries_bidding_zones, sheet_name = 0, engine = 'openpyxl')
        df_coutries_bidding_zones = df_coutries_bidding_zones[
            dict_columns_coutries_bidding_zones.values()
        ]
        
        dict_coutry_code_mapping = dict(zip(
            df_coutries_bidding_zones['CountryCode'], 
            df_coutries_bidding_zones['Country']
        ))
        
        value = parser.get('sftp_entsoe', 'data_types_to_sync')
        list_data_types_to_sync = list(filter(None, (x.strip() for x in value.splitlines())))
                
        ip_mongo_server = parser.get('db_connection', 'ip_mongo_server')
        library_name = parser.get('db_connection', 'library_name_entsoe')
    """
    except IOError as err:
        logger.error('Problem opening configuration file %s. %s' % (config_file_entsoe, err))
        return -1
    except configparser.NoOptionError as err:
        logger.error('Problem with configuration file %s. %s' % (config_file_entsoe, err))
        return -1
    except configparser.NoSectionError as err:
        logger.error('Problem with configuration file %s. %s' % (config_file_entsoe, err))
        return -1
    except:
        logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        logger.info('Problem with configuration file %s.' % (config_file_entsoe))
        return -1
    """
    #-----------------------------------------------------------------------------------
    # Init Arctic/MongoDB Library
    lib = initialize_library(
        ip_mongo_server = ip_mongo_server,
        library_name = library_name,
    )
    
    #-------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------
    if not os.path.exists(config_file_entsoe_data_types):
        logger.error('Cannot find configuration file %s' % (config_file_entsoe_data_types))
    
    logger.info('Reading configuration file {}'.format(config_file_entsoe_data_types))
    parser = configparser.SafeConfigParser()
    
    # make the parser case sensitive (by default case insensitive)
    parser.optionxform = str
    
    #try:
    if True:
        # read config file
        parser.read(config_file_entsoe_data_types)
        
        date_format = parser.get('update', 'date_format')
        first_date = parser.get('update', 'first_date')
        first_date = pd.to_datetime(first_date, format=date_format)
        
        # curves configurations
        list_data_types = list()
        for section in parser.sections():
            
            if section != 'update':
                data_format = parser.get(section, 'data_format')
                
                entsoe_data_type = EntsoeDataType()
                
                update_modified_files = parser.getboolean(section, 'update_modified_files')
                update_full_history = parser.getboolean(section, 'update_full_history')
                last_update = parser.get(section, 'last_update')
                last_update = pd.to_datetime(last_update, format=date_format)
            
                if data_format == 'country':
                    
                    columns_values = parser.get(section, 'columns_values')
                    columns_values = list(filter(None, (x.strip() for x in columns_values.split(','))))
                    
                    columns_rename = parser.get(section, 'columns_rename')
                    columns_rename = list(filter(None, (x.strip() for x in columns_rename.split(','))))
                    
                    column_AreaName = parser.get(section, 'column_AreaName')
                    column_MapCode = parser.get(section, 'column_MapCode')
                    column_DateTime = parser.get(section, 'column_DateTime')
                    
                    countries = parser.get(section, 'countries')
                    countries = list(filter(None, (x.strip() for x in countries.split(','))))
                    
                    entsoe_data_type = EntsoeDataTypeCountry(
                        columns_values = columns_values,
                        columns_rename = columns_rename,
                        data_format = data_format,
                        symbol_name = section,
                        arctic_lib = lib,
                        chunk_size = 'D', # TODO
                        configfile_last_update = config_file_entsoe_data_types,
                        local_dir = local_dir,
                        last_update = last_update,
                        update_full_history = update_full_history,
                        first_date = first_date,
                        date_format = date_format,
                        update_modified_files = update_modified_files,
                        
                        column_AreaName = column_AreaName,
                        column_MapCode = column_MapCode,
                        column_DateTime = column_DateTime,
                        countries = countries,
                        
                        dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
                        df_coutries_bidding_zones = df_coutries_bidding_zones,
                        dict_coutry_code_mapping = dict_coutry_code_mapping,
                    )
                                
                elif data_format == 'country_group':
                    
                    columns_values = parser.get(section, 'columns_values')
                    columns_values = list(filter(None, (x.strip() for x in columns_values.split(','))))
                    
                    columns_rename = parser.get(section, 'columns_rename')
                    columns_rename = list(filter(None, (x.strip() for x in columns_rename.split(','))))
                    
                    column_AreaName = parser.get(section, 'column_AreaName')
                    column_MapCode = parser.get(section, 'column_MapCode')
                    column_DateTime = parser.get(section, 'column_DateTime')
                    column_group = parser.get(section, 'column_group')
                    
                    countries = parser.get(section, 'countries')
                    countries = list(filter(None, (x.strip() for x in countries.split(','))))
                    
                    entsoe_data_type = EntsoeDataTypeCountryGroup(
                        columns_values = columns_values,
                        columns_rename = columns_rename,
                        column_AreaName = column_AreaName,
                        column_MapCode = column_MapCode,
                        column_DateTime = column_DateTime,
                        
                        data_format = data_format,
                        symbol_name = section,
                        arctic_lib = lib,
                        chunk_size = 'D', # TODO
                        configfile_last_update = config_file_entsoe_data_types,
                        local_dir = local_dir,
                        last_update = last_update,
                        update_full_history = update_full_history,
                        first_date = first_date,
                        date_format = date_format,
                        update_modified_files = update_modified_files,
                        column_group = column_group,
                        countries = countries,
                        
                        dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
                        df_coutries_bidding_zones = df_coutries_bidding_zones,
                        dict_coutry_code_mapping = dict_coutry_code_mapping,
                    )
                        
                elif data_format == 'flow':
                    
                    columns_values = parser.get(section, 'columns_values')
                    columns_values = list(filter(None, (x.strip() for x in columns_values.split(','))))
                    
                    columns_rename = parser.get(section, 'columns_rename')
                    columns_rename = list(filter(None, (x.strip() for x in columns_rename.split(','))))
                    
                    column_InAreaName = parser.get(section, 'column_InAreaName')
                    column_InMapCode = parser.get(section, 'column_InMapCode')
                    column_OutAreaName = parser.get(section, 'column_OutAreaName')
                    column_OutMapCode = parser.get(section, 'column_OutMapCode')
                    column_DateTime = parser.get(section, 'column_DateTime')
                    
                    flows = parser.get(section, 'flows')
                    flows = list(filter(None, (x.strip() for x in flows.split("\n"))))
                    flows = list([x.strip() for x in l.split(',')] for l in flows)
                        
                    entsoe_data_type = EntsoeDataTypeFlow(
                        columns_values = columns_values,
                        columns_rename = columns_rename,
                        data_format = data_format,
                        symbol_name = section,
                        arctic_lib = lib,
                        chunk_size = 'D', # TODO
                        configfile_last_update = config_file_entsoe_data_types,
                        local_dir = local_dir,
                        last_update = last_update,
                        update_full_history = update_full_history,
                        first_date = first_date,
                        date_format = date_format,
                        update_modified_files = update_modified_files,
                        column_InAreaName = column_InAreaName,
                        column_InMapCode = column_InMapCode,
                        column_OutAreaName = column_OutAreaName,
                        column_OutMapCode = column_OutMapCode,
                        column_DateTime = column_DateTime,
                        flows = flows,
                        
                        dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
                        df_coutries_bidding_zones = df_coutries_bidding_zones,
                        dict_coutry_code_mapping = dict_coutry_code_mapping,
                    )
                else:
                    pass
                
                list_data_types.append(entsoe_data_type)
    """    
    except IOError as err:
        logger.error('Problem opening configuration file %s. %s' % (config_file_entsoe_data_types, err))
        return -1
    except configparser.NoOptionError as err:
        logger.error('Problem with configuration file %s. %s' % (config_file_entsoe_data_types, err))
        return -1
    except configparser.NoSectionError as err:
        logger.error('Problem with configuration file %s. %s' % (config_file_entsoe_data_types, err))
        return -1
    except:
        logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        logger.info('Problem with configuration file %s.' % (config_file_entsoe_data_types))
        return -1
    """
    return (
        hostname,
        username,
        password,
        remote_dir,
        local_dir,
        list_data_types_to_sync,
        
        ip_mongo_server, 
        library_name,
        
        #update_full_history,
        #first_date,
        #date_format,
        list_data_types,
    )
   