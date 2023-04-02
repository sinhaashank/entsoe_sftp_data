import pandas as pd
from datetime import datetime, timezone
import logging

#logger = logging.getLogger(__name__)
logger = logging.getLogger('entsoe')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)
###############################################################################################

#----------------------------------------------------------------------------------------------
def read_csv_entsoe(
    filename = None,
):
    logger.info("Reading csv file '{}'...".format(filename))
    df = pd.read_csv(
        filename,
        sep='\t',
        #sep='\s+',
        header=0,
        encoding="UTF-8" #UTF-16
    )
    
    return df

#----------------------------------------------------------------------------------------------
def filter_data_by_country(
    df = None,
    df_coutries_bidding_zones = None,
    dict_columns_coutries_bidding_zones = None,
    year = None,
    month = None,
    country_code = None,
    column_AreaName = 'AreaName',
    column_MapCode = 'MapCode',
):
    from datetime import datetime
    import pandas as pd

    date = datetime(year=year, month=month, day=1)

    col_CountryCode = dict_columns_coutries_bidding_zones['column_CountryCode']
    col_change_date = dict_columns_coutries_bidding_zones['column_change_date']
    col_AreaName = dict_columns_coutries_bidding_zones['column_AreaName']
    col_MapCode = dict_columns_coutries_bidding_zones['column_MapCode']
    
    area_name, map_code = tuple(
        df_coutries_bidding_zones
        .loc[df_coutries_bidding_zones[col_CountryCode] == country_code]
        .set_index(col_change_date)
        .sort_index()
        .asof(date) # returns series
        [[col_AreaName, col_MapCode]].tolist()
    )
        
    #--------------------------------------------------------------------------------------------
    df_focus = df.loc[
        (df[column_AreaName] == area_name)
        & (df[column_MapCode] == map_code)
    ]
    
    return df_focus

#################################################################################################
#------------------------------------------------------------------------------------------------
class EntsoeDataType:
    def __init__(
        self,
        columns_values = None,
        columns_rename = None,
        data_format = None,
        symbol_name = None,
        arctic_lib = None,
        chunk_size = None,
        configfile_last_update = None,
        local_dir = None,
        last_update = None,
        update_full_history = None,
        first_date = None,
        date_format = None,
        update_modified_files = None,
        dict_columns_coutries_bidding_zones = None,
        df_coutries_bidding_zones = None,
        dict_coutry_code_mapping = None,
    ):
        self.columns_values = columns_values
        self.columns_rename = columns_rename

        self.data_format = data_format
        self.symbol_name = symbol_name
        self.arctic_lib = arctic_lib
        self.chunk_size = chunk_size
        self.configfile_last_update = configfile_last_update
        self.local_dir = local_dir
        self.last_update = last_update
        self.update_full_history = update_full_history
        self.first_date = first_date
        self.date_format = date_format
        self.update_modified_files = update_modified_files
        
        self.list_months_to_update = list()
        self.dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones
        self.df_coutries_bidding_zones = df_coutries_bidding_zones
        self.dict_coutry_code_mapping = dict_coutry_code_mapping
    
    #----------------------------------------------------------------------------------------------
    def set_months_to_update(
        self, 
        dict_modified_files = None,
    ):
        import glob
        import os
        from datetime import datetime
        
        list_year_month = list()
        
        if self.update_modified_files == True:
            list_modified_files = list()
            if self.symbol_name in dict_modified_files:
                list_modified_files = dict_modified_files[self.symbol_name]
            
            if len(list_modified_files) > 0:
                for filename in list_modified_files:
                    month = int(filename.split('_')[1])
                    #month = (filename.split('_')[1])
                    year = int(filename.split('_')[0])
                    
                    list_year_month.append((
                        year, 
                        month, 
                        os.path.join(self.local_dir, self.symbol_name,filename)
                    ))
                
                list_year_month.sort(key=lambda tup: (tup[0], tup[1]))
            
            if len(list_year_month) > 0:
                logger.info("Modified months: {}".format(list_year_month))
            self.list_months_to_update = list_year_month
            return
            
        #---------------------------------------------------------------------------------------
        if self.update_full_history:
            start_date = self.first_date
        else:
            start_date = self.last_update
            
        latest_available_file = max(
            glob.glob(os.path.join(self.local_dir, self.symbol_name,  '*' + self.symbol_name + '.csv')),
            key = lambda name: (
                int(os.path.basename(name).split('_')[0]),
                int(os.path.basename(name).split('_')[1])
            ))
        
        latest_available_year = int(os.path.basename(latest_available_file).split("_")[0])
        latest_available_month = int(os.path.basename(latest_available_file).split("_")[1])
        
        list_year_month = pd.date_range(
            start_date,
            datetime(year=latest_available_year, month=latest_available_month, day=1), 
            freq='D', #freq='MS'
        ).map(lambda x: (x.year, x.month)).unique().to_list()
        
        
        self.list_months_to_update = [
            (year, month, os.path.join(
                self.local_dir, 
                self.symbol_name,  
                "{}_{}_{}.csv".format(year, str(month).zfill(2), self.symbol_name)))
            for year, month in list_year_month
        ]
        
        logger.info(
            "Updating data since {}"
            .format(start_date.strftime(self.date_format))
        )
        
        return

    #----------------------------------------------------------------------------------------------
    def extract_data(
        self,
        df = None,
        year = None,
        month = None,
    ):
        pass
    
    #----------------------------------------------------------------------------------------------
    def update_data_type(self):
        """
        The default behaviour applies for country and coutry_group
        """
        import glob
        import os
        from datetime import datetime
        from config_utils import config_set_option, config_get_option_list
        
        if len(self.list_months_to_update) == 0:
            return

        #---------------------------------------------------------------------------------------
        for year, month, filename in self.list_months_to_update:
            
            if os.path.exists(filename):
                df_raw = read_csv_entsoe(filename = filename)
            
                for country_code in self.countries:
                    
                    symbol_name = self.symbol_name + '_' + self.dict_coutry_code_mapping[country_code]
                    
                    df_country =  self.extract_data(
                        df = df_raw,
                        year = year,
                        month = month,
                        country_code = country_code,
                    )
                                        
                    if len(df_country) > 0:
                        logger.info(
                            'Updating Symbol: {}, {} {}, shape: {}'
                            .format(symbol_name, month, year, df_country.shape)
                        )
                                                
                        self.arctic_lib.update(
                            symbol_name, 
                            df_country, 
                            upsert=True, 
                            chunk_size=self.chunk_size, 
                            #metadata={'prev_update_year': year},
                        )
                    else:
                        logger.warning(
                            'Empty Dataframe: {}, {} {}'
                            .format(symbol_name, month, year)
                        )
                        
                # update latest update month
                prev_update_month = config_set_option(
                    file_name = self.configfile_last_update,
                    section = self.symbol_name,
                    option = 'last_update',
                    option_value = datetime(year, month, 1).strftime(self.date_format),
                )
                
            else:
                logger.warning("File not found: '{}'".format(filename))

#################################################################################################
#------------------------------------------------------------------------------------------------
class EntsoeDataTypeCountry(EntsoeDataType):
    def __init__(
        self,
        columns_values = None,
        columns_rename = None,

        data_format = None,
        symbol_name = None,
        arctic_lib = None,
        chunk_size = None,
        configfile_last_update = None,
        local_dir = None,
        last_update = None,
        update_full_history = None,
        first_date = None,
        date_format = None,
        update_modified_files = None,
        
        column_AreaName = None,
        column_MapCode = None,
        column_DateTime = None,
        countries = None,
        dict_columns_coutries_bidding_zones = None,
        df_coutries_bidding_zones = None,
        dict_coutry_code_mapping = None,
    ):
        EntsoeDataType.__init__(
            self,
            columns_values = columns_values,
            columns_rename = columns_rename,
            data_format = data_format,
            symbol_name = symbol_name,
            arctic_lib = arctic_lib,
            chunk_size = chunk_size,
            configfile_last_update = configfile_last_update,
            local_dir = local_dir,
            last_update = last_update,
            update_full_history = update_full_history,
            first_date = first_date,
            date_format = date_format,
            update_modified_files = update_modified_files,
            dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
            df_coutries_bidding_zones = df_coutries_bidding_zones,
            dict_coutry_code_mapping = dict_coutry_code_mapping,
        )
        self.column_AreaName = column_AreaName
        self.column_MapCode = column_MapCode
        self.column_DateTime = column_DateTime
        self.countries = countries
        
    #------------------------------------------------------------------------------------------    
    def extract_data(
        self,
        df = None,
        year = None,
        month = None,
        country_code = None,
    ):
        """
        DayAheadPrices, Price
        ActualTotalLoad, TotalLoadValue
        
        """
       
        from datetime import datetime
        #from entsoe_countries import filter_data_by_country
                
        df_focus = filter_data_by_country(
            df = df,
            df_coutries_bidding_zones = self.df_coutries_bidding_zones,
            dict_columns_coutries_bidding_zones = self.dict_columns_coutries_bidding_zones,
            year = year,
            month = month,
            country_code = country_code,
            column_AreaName = self.column_AreaName,
            column_MapCode = self.column_MapCode,
        )
                
        dict_rename = {
            x: y + '_' + self.dict_coutry_code_mapping[country_code] 
            for x, y in zip(self.columns_values, self.columns_rename)
        }
        dict_rename[self.column_DateTime] = 'date'
        
        df_focus = (
            df_focus[[self.column_DateTime] + self.columns_values]
            .sort_values(self.column_DateTime)
            .rename(columns=dict_rename)
            .set_index('date')
        )
        # Converting the index to date
        df_focus.index = pd.to_datetime(df_focus.index, format='%Y-%m-%d %H:%M:%S.%f')
        
        return df_focus

#################################################################################################
#------------------------------------------------------------------------------------------------
class EntsoeDataTypeCountryGroup(EntsoeDataType):
    def __init__(
        self,
        columns_values = None,
        columns_rename = None,
        data_format = None,
        symbol_name = None,
        arctic_lib = None,
        chunk_size = None,
        configfile_last_update = None,
        local_dir = None,
        last_update = None,
        update_full_history = None,
        first_date = None,
        date_format = None,
        update_modified_files = None,
        
        column_AreaName = None,
        column_MapCode = None,
        column_DateTime = None,
        column_group = None,
        countries = None,
        dict_columns_coutries_bidding_zones = None,
        df_coutries_bidding_zones = None,
        dict_coutry_code_mapping = None,
    ):
        EntsoeDataType.__init__(
            self,
            columns_values = columns_values,
            columns_rename = columns_rename,
            data_format = data_format,
            symbol_name = symbol_name,
            arctic_lib = arctic_lib,
            chunk_size = chunk_size,
            configfile_last_update = configfile_last_update,
            local_dir = local_dir,
            last_update = last_update,
            update_full_history = update_full_history,
            first_date = first_date,
            date_format = date_format,
            update_modified_files = update_modified_files,
            dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
            df_coutries_bidding_zones = df_coutries_bidding_zones,
            dict_coutry_code_mapping = dict_coutry_code_mapping,
        )
        self.column_AreaName = column_AreaName
        self.column_MapCode = column_MapCode
        self.column_DateTime = column_DateTime
        self.column_group = column_group
        self.countries = countries
        
    #------------------------------------------------------------------------------------------
    def extract_data(
        self,
        df = None,
        year = None,
        month = None,
        country_code = None,
    ):
        """
        AggregatedGenerationPerType, ProductionType, ActualGenerationOutput
        InstalledGenerationCapacityAggregated, ProductionType, AggregatedInstalledCapacity
        """
        
        from datetime import datetime
        #from entsoe_countries import filter_data_by_country
                    
        df_focus = filter_data_by_country(
            df = df,
            df_coutries_bidding_zones = self.df_coutries_bidding_zones,
            dict_columns_coutries_bidding_zones = self.dict_columns_coutries_bidding_zones,
            year = year,
            month = month,
            country_code = country_code,
            column_AreaName = self.column_AreaName,
            column_MapCode = self.column_MapCode,
        )

        #------------------------------------------------------------------------------------------
        #df_focus = df_focus[
        #    [self.column_DateTime, self.column_group] + self.columns_values 
        #].sort_values(self.column_DateTime)
        
        #df_focus.index = pd.to_datetime(df_focus.index)
        #return df_focus
        len1 = len(df_focus)
        df_focus = (
            df_focus[[self.column_DateTime, self.column_group] + self.columns_values]
            .sort_values(self.column_DateTime)
            .drop_duplicates([self.column_DateTime, self.column_group], keep='first')
            #.set_index([self.column_DateTime, self.column_group]) #append=True
        )
        
        len2 = len(df_focus)
        if len2 < len1:
            logger.warning(
                "{} cases found with duplicated indices. Duplicated indices are ignored"
                .format(len2 - len1)
            )
        
        df_unstacked = (
            df_focus
            .set_index([
                self.column_DateTime, self.column_group],
                #append=True,
            )
            .unstack(level = [self.column_group])
        )
        
        list_rename = [x + ' ' + self.dict_coutry_code_mapping[country_code] for x in self.columns_rename]
        df_unstacked.columns.set_levels(list_rename, level=0, inplace=True)
        
        df_unstacked.columns = df_unstacked.columns.map(' '.join).str.strip('')
        
        #df_unstacked = df_unstacked.droplevel(0, axis=1)
        #df_unstacked.columns.name = None
        
        df_unstacked.index.names = ['date']
        
        # Converting the index to date
        df_unstacked.index = pd.to_datetime(df_unstacked.index, format='%Y-%m-%d %H:%M:%S.%f')
        
        return df_unstacked
    
#################################################################################################
#------------------------------------------------------------------------------------------------
class EntsoeDataTypeFlow(EntsoeDataType):
    def __init__(
        self,
        columns_values = None,
        columns_rename = None,
        data_format = None,
        symbol_name = None,
        arctic_lib = None,
        chunk_size = None,
        configfile_last_update = None,
        local_dir = None,
        last_update = None,
        update_full_history = None,
        first_date = None,
        date_format = None,
        update_modified_files = None,
        
        column_InAreaName = None,
        column_InMapCode = None,
        column_OutAreaName = None,
        column_OutMapCode = None,
        column_DateTime = None,
        flows = None,
        dict_columns_coutries_bidding_zones = None,
        df_coutries_bidding_zones = None,
        dict_coutry_code_mapping = None,
    ):
        EntsoeDataType.__init__(
            self,
            columns_values = columns_values,
            columns_rename = columns_rename,
            data_format = data_format,
            symbol_name = symbol_name,
            arctic_lib = arctic_lib,
            chunk_size = chunk_size,
            configfile_last_update = configfile_last_update,
            local_dir = local_dir,
            last_update = last_update,
            update_full_history = update_full_history,
            first_date = first_date,
            date_format = date_format,
            update_modified_files = update_modified_files,
            dict_columns_coutries_bidding_zones = dict_columns_coutries_bidding_zones,
            df_coutries_bidding_zones = df_coutries_bidding_zones,
            dict_coutry_code_mapping = dict_coutry_code_mapping,
        )
        self.column_InAreaName = column_InAreaName
        self.column_InMapCode = column_InMapCode
        self.column_OutAreaName = column_OutAreaName
        self.column_OutMapCode = column_OutMapCode
        self.column_DateTime = column_DateTime
        self.flows = flows
        
    #------------------------------------------------------------------------------------------
    def extract_data(
        self,
        df = None,
        year = None,
        month = None,
        country_code_in = None,
        country_code_out = None,
    ):
        """
        DayAheadCommercialSchedules, Capacity
            
        """
        
        from datetime import datetime
        #from entsoe_countries import filter_data_by_country
                
        df_focus = filter_data_by_country(
            df = df,
            df_coutries_bidding_zones = self.df_coutries_bidding_zones,
            dict_columns_coutries_bidding_zones = self.dict_columns_coutries_bidding_zones,
            year = year,
            month = month,
            country_code = country_code_out,
            column_AreaName = self.column_OutAreaName,
            column_MapCode = self.column_OutMapCode,
        )
        
        df_focus = filter_data_by_country(
            df = df_focus,
            df_coutries_bidding_zones = self.df_coutries_bidding_zones,
            dict_columns_coutries_bidding_zones = self.dict_columns_coutries_bidding_zones,
            year = year,
            month = month,
            country_code = country_code_in,
            column_AreaName = self.column_InAreaName,
            column_MapCode = self.column_InMapCode,
        )
                
        dict_rename = {
            x: (y 
                + '_' + self.dict_coutry_code_mapping[country_code_out] 
                + '_' + self.dict_coutry_code_mapping[country_code_in]
               )
            for x, y in zip(self.columns_values, self.columns_rename)
        }
        dict_rename[self.column_DateTime] = 'date'
        
        df_focus = (
            df_focus[[self.column_DateTime] + self.columns_values]
            .sort_values(self.column_DateTime)
            .rename(columns = dict_rename)
            .set_index('date')
        )
        # Converting the index to date
        df_focus.index = pd.to_datetime(df_focus.index, format='%Y-%m-%d %H:%M:%S.%f')
        
        return df_focus
    
    #------------------------------------------------------------------------------------------
    #------------------------------------------------------------------------------------------
    def update_data_type(self):
        import glob
        import os
        from datetime import datetime
        from config_utils import config_set_option
        
        if len(self.list_months_to_update) == 0:
            return

        #---------------------------------------------------------------------------------------
        for year, month, filename in self.list_months_to_update:
            
            if os.path.exists(filename):
                df_raw = read_csv_entsoe(filename = filename)
            
                for country_code_out, country_code_in in self.flows:
                    
                    symbol_name = (
                        self.symbol_name 
                        + '_' + self.dict_coutry_code_mapping[country_code_out] 
                        + '_' + self.dict_coutry_code_mapping[country_code_in]
                    )
                    
                    df_flow =  self.extract_data(
                        df = df_raw,
                        year = year,
                        month = month,
                        country_code_in = country_code_in,
                        country_code_out = country_code_out,
                    )
                    
                    if len(df_flow) > 0:
                        logger.info(
                            'Updating Symbol: {}, {} {}, shape: {}'
                            .format(symbol_name, month, year, df_flow.shape)
                        )
                        
                        #return df_country
                        
                        self.arctic_lib.update(
                            symbol_name, 
                            df_flow, 
                            upsert=True, 
                            chunk_size=self.chunk_size, 
                            #metadata={'prev_update_year': year},
                        )
                    else:
                        logger.warning(
                            'Empty Dataframe: {}, {} {}'
                            .format(symbol_name, month, year)
                        )
                        
                # update latest update month
                prev_update_month = config_set_option(
                    file_name = self.configfile_last_update,
                    section = self.symbol_name,
                    option = 'last_update',
                    option_value = datetime(year, month, 1).strftime(self.date_format),
                )
                
            else:
                logger.warning("File not found: '{}'".format(filename))
        
    
        