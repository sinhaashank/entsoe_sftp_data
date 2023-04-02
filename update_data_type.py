import pandas as pd
from datetime import datetime
import logging

#logger = logging.getLogger(__name__)
logger = logging.getLogger('entsoe')
#logger = logging.getLogger('logger_name')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)

############################################################################################
def read_csv_entsoe(
    filename = None,
):
    logger.info("Reading csv file '{}'...".format(filename))
    df = pd.read_csv(
        filename, 
        sep='\t',
        #sep='\s+',
        header=0,
        encoding="UTF-8" #changed from UTF-16
    )
    
    return df

###########################################################################################
def update_data_type_by_country(
    symbol_name = None,
    arctic_lib = None,
    chunk_size = 'D',
    data_type = None,
    local_dir = None,
    configfile_last_update = None,
    column_value = None,
):
    import glob
    import os
    from datetime import datetime
    from config_utils import config_get_option, config_set_option, config_get_option_list
    from entsoe_extract_data_from_csv import extract_data_by_country
    
    #--------------------------------------------------------------------------------------
    # get the month of the previous update
    prev_update_year = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_year',
    ))
    
    prev_update_month = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_month',
    ))
    
    list_countries = config_get_option_list(
        file_name = configfile_last_update,
        section = data_type,
        option = 'countries',
        sep = ',',
    )
        
    #---------------------------------------------------------------------------------------
    latest_available_file = os.path.basename(max(glob.glob(os.path.join(
        local_dir, data_type,  '*' + data_type + '.csv'))))
    
    latest_available_year = int(latest_available_file.split("_")[0])
    latest_available_month = int(latest_available_file.split("_")[1])
    
    list_year_month = pd.date_range(
        datetime(year=prev_update_year, month=prev_update_month, day=1),
        datetime(year = latest_available_year, month = latest_available_month, day=1),
        freq='MS'
    ).map(lambda x: (x.year, x.month)).to_list()
    
    #---------------------------------------------------------------------------------------
    for year, month in list_year_month:
        filename = os.path.join(
            local_dir, data_type,  
            str(year) + '_' + str(month).zfill(2) + '_' + data_type + '.csv'
        )
        
        if os.path.exists(filename):
            df_raw = read_csv_entsoe(filename = filename)
        
            for country_code in list_countries:
                
                symbol_name = data_type + '_' + country_code
                
                df_country =  extract_data_by_country(
                    df = df_raw,
                    year = year,
                    month = month,
                    country_code = country_code,
                    data_type = data_type,
                    column_value = column_value,
                )
                
                if len(df_country) > 0:
                    logger.info(
                        'Updating Symbol: {}, {} {}, shape: {}'
                        .format(symbol_name, month, year, df_country.shape)
                    )
                    
                    #return df_country
                    
                    arctic_lib.update(
                        symbol_name, 
                        df_country, 
                        upsert=True, 
                        chunk_size=chunk_size, 
                        #metadata={'prev_update_year': year},
                    )
                else:
                    logger.warning(
                        'Empty Dataframe: {}, {} {}'
                        .format(symbol_name, month, year)
                    )
                    
            # update latest update month
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_year',
                option_value = str(year),
            )
            
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_month',
                option_value = str(month),
            )
            
        else:
            logger.warning("File not found: '{}'".format(filename))

###########################################################################################
def update_data_type_by_country_by_group(
    arctic_lib = None,
    chunk_size = 'D',
    data_type = None,
    local_dir = None,
    configfile_last_update = None,
    column_AreaName = 'AreaName',
    column_MapCode = 'MapCode',
    column_DateTime = 'DateTime',
    column_value = None,
    column_group = None,
    label_prefix = None,
):
    import glob
    import os
    from datetime import datetime
    from config_utils import config_get_option, config_set_option, config_get_option_list
    from entsoe_extract_data_from_csv import extract_data_by_country_by_group
    
    #--------------------------------------------------------------------------------------
    # get the month of the previous update
    prev_update_year = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_year',
    ))
    
    prev_update_month = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_month',
    ))
    
    list_countries = config_get_option_list(
        file_name = configfile_last_update,
        section = data_type,
        option = 'countries',
        sep = ',',
    )
        
    #---------------------------------------------------------------------------------------
    latest_available_file = os.path.basename(max(glob.glob(os.path.join(
        local_dir, data_type,  '*' + data_type + '.csv'))))
    
    latest_available_year = int(latest_available_file.split("_")[0])
    latest_available_month = int(latest_available_file.split("_")[1])
    
    list_year_month = pd.date_range(
        datetime(year=prev_update_year, month=prev_update_month, day=1),
        datetime(year = latest_available_year, month = latest_available_month, day=1),
        freq='MS'
    ).map(lambda x: (x.year, x.month)).to_list()
    
    #---------------------------------------------------------------------------------------
    for year, month in list_year_month:
        filename = os.path.join(
            local_dir, data_type,  
            str(year) + '_' + str(month).zfill(2) + '_' + data_type + '.csv'
        )
        
        if os.path.exists(filename):
            df_raw = read_csv_entsoe(filename = filename)
        
            for country_code in list_countries:
                
                symbol_name = data_type + '_' + country_code
                
                df_country =  extract_data_by_country_by_group(
                    df = df_raw,
                    year = year,
                    month = month,
                    data_type = data_type,
                    country_code = country_code,
                    column_AreaName = column_AreaName,
                    column_MapCode = column_MapCode,
                    column_DateTime = column_DateTime,
                    column_value = column_value,
                    column_group = column_group,
                    label_prefix = label_prefix + country_code + ' ',
                )
                
                if len(df_country) > 0:
                    logger.info(
                        'Updating Symbol: {}, {} {}, shape: {}'
                        .format(symbol_name, month, year, df_country.shape)
                    )
                                        
                    arctic_lib.update(
                        symbol_name, 
                        df_country, 
                        upsert=True, 
                        chunk_size=chunk_size, 
                        #metadata={'prev_update_year': year},
                    )
                else:
                    logger.warning(
                        'Empty Dataframe: {}, {} {}'
                        .format(symbol_name, month, year)
                    )
                    
            # update latest update month
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_year',
                option_value = str(year),
            )
            
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_month',
                option_value = str(month),
            )
            
        else:
            logger.warning("File not found: '{}'".format(filename))
            
###########################################################################################
def update_data_type_by_flow(
    arctic_lib = None,
    chunk_size = 'D',
    data_type = None,
    local_dir = None,
    configfile_last_update = None,
    column_value = None,    
    column_InAreaName = 'InAreaName',
    column_InMapCode = 'InMapCode',
    column_OutAreaName = 'OutAreaName',
    column_OutMapCode = 'OutMapCode',
    column_DateTime = 'DateTime',
):
    import glob
    import os
    from datetime import datetime
    from config_utils import config_get_option, config_set_option, config_get_option_list, config_get_option_list_of_lists
    from entsoe_extract_data_from_csv import extract_data_flows
    
    #--------------------------------------------------------------------------------------
    # get the month of the previous update
    prev_update_year = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_year',
    ))
    
    prev_update_month = int(config_get_option(
        file_name = configfile_last_update,
        section = data_type,
        option = 'prev_update_month',
    ))
    
    list_flows = config_get_option_list_of_lists(
        file_name = configfile_last_update,
        section = data_type,
        option = 'flows',
        sep1 = '\n',
        sep2 = ',',
    )
        
    #---------------------------------------------------------------------------------------
    latest_available_file = os.path.basename(max(glob.glob(os.path.join(
        local_dir, data_type,  '*' + data_type + '.csv'))))
    
    latest_available_year = int(latest_available_file.split("_")[0])
    latest_available_month = int(latest_available_file.split("_")[1])
    
    list_year_month = pd.date_range(
        datetime(year=prev_update_year, month=prev_update_month, day=1),
        datetime(year = latest_available_year, month = latest_available_month, day=1),
        freq='MS'
    ).map(lambda x: (x.year, x.month)).to_list()
    
    #---------------------------------------------------------------------------------------
    for year, month in list_year_month:
        filename = os.path.join(
            local_dir, data_type,  
            str(year) + '_' + str(month).zfill(2) + '_' + data_type + '.csv'
        )
        
        if os.path.exists(filename):
            df_raw = read_csv_entsoe(filename = filename)
        
            for country_code_out, country_code_in in list_flows:
                
                symbol_name = data_type + '_' + country_code_out + '_' + country_code_in
                
                df_flow =  extract_data_flows(
                    df = df_raw,
                    year = year,
                    month = month,
                    country_code_in = country_code_in,
                    country_code_out = country_code_out,
                    data_type = data_type,
                    column_value = column_value,
                    column_InAreaName = column_InAreaName,
                    column_InMapCode = column_InMapCode,
                    column_OutAreaName = column_OutAreaName,
                    column_OutMapCode = column_OutMapCode,
                    column_DateTime = column_DateTime,
                )
                
                if len(df_flow) > 0:
                    logger.info(
                        'Updating Symbol: {}, {} {}, shape: {}'
                        .format(symbol_name, month, year, df_flow.shape)
                    )
                    
                    #return df_country
                    
                    arctic_lib.update(
                        symbol_name, 
                        df_flow, 
                        upsert=True, 
                        chunk_size=chunk_size, 
                        #metadata={'prev_update_year': year},
                    )
                else:
                    logger.warning(
                        'Empty Dataframe: {}, {} {}'
                        .format(symbol_name, month, year)
                    )
                    
            # update latest update month
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_year',
                option_value = str(year),
            )
            
            prev_update_month = config_set_option(
                file_name = configfile_last_update,
                section = data_type,
                option = 'prev_update_month',
                option_value = str(month),
            )
            
        else:
            logger.warning("File not found: '{}'".format(filename))
   