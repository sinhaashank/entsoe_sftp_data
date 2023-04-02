import logging

#logger = logging.getLogger(__name__)
logger = logging.getLogger('entsoe')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)

############################################################################################
def initialize_library(
    ip_mongo_server = None,
    library_name = None,
):
    from arctic import Arctic, CHUNK_STORE
    
    conn = Arctic(ip_mongo_server)
    
    logger.info ('Arctic, Existing Libraries: {}'.format(conn.list_libraries()))
    
    if not conn.library_exists(library_name):
        logger.info('Creating Library {}'.format(library_name))
        conn.initialize_library(library_name, lib_type=CHUNK_STORE)
    
    logger.info('Init Connection to Library {}'.format(library_name))
    lib = conn[library_name]
    logger.info ('Library {}, Existing Symbols: {}'.format(library_name, lib.list_symbols()))
        
    return lib

############################################################################################
def store_df_single_column(
    df = None,           # pandas time series
    lib = None,
    symbol_name = None,  # eg. 'day_ahead_prices_BE'
    meta_data = None,    # dict
    chunk_size = 'D', 
):  
    """
    
    """
    assert df.shape[1] == 1
    
    # date index should be named date or create column date
    # df = df.reset_index().rename(columns={0: 'date'})
    df.index.names = ['date']
    
    logger.info('Updating Symbol: {}'.format(symbol_name))
    lib.update(symbol_name, df, upsert=True, chunk_size=chunk_size)
    
    #read_metadata(self, symbol)
    print(lib.get_info(symbol_name))
    print(lib.stats())
    
############################################################################################
def store_df_column_by_column(
    df = None,           # pandas time series
    lib = None,
    meta_data = None,    # dict
    chunk_size = 'D', 
):  
    """
    
    """
    assert df.shape[1] >= 1
    
    # date index should be named date or create column date
    # df = df.reset_index().rename(columns={0: 'date'})
    df.index.names = ['date']
    
    for column in df.columns.values:
        symbol_name = column
        lib.update(symbol_name, df, upsert=True, chunk_size=chunk_size)

############################################################################################
def store_df_all_columns(
    df = None,           # pandas df
    lib = None,
    symbol_name = None,  # 
    meta_data = None,    # dict
    chunk_size = 'D', 
):  
    """
    
    """
    assert df.shape[1] >= 1
    
    # date index should be named date or create column date
    # df = df.reset_index().rename(columns={0: 'date'})
    df.index.names = ['date']
     
    lib.update(symbol_name, df, upsert=True, chunk_size=chunk_size)
    
    
    