
###########################################################################################
def filter_data_by_country(
    df = None,
    year = None,
    month = None,
    country_code = None,
    column_AreaName = 'AreaName',
    column_MapCode = 'MapCode',
):
    from datetime import datetime
    import pandas as pd

    date = datetime(year=year, month=month, day=1)
    date_oct_2018 = datetime(year=2018, month=10, day=1)
    
    df_focus = pd.DataFrame()
    
    #-------------------------------------------------------------------------------------
    if country_code == 'DE':
        if date < date_oct_2018:
            df_focus = df.loc[
                (df[column_AreaName] == 'DE-AT-LU BZN')
                & (df[column_MapCode] == 'DE_AT_LU')
            ]    
            
        else:
            df_focus = df.loc[
                (df[column_AreaName] == 'DE-LU BZN')
                & (df[column_MapCode] == 'DE_LU')
            ]
    
    #-------------------------------------------------------------------------------------
    elif country_code == 'AT':
        if date < date_oct_2018:
            df_focus = df.loc[
                (df[column_AreaName] == 'DE-AT-LU BZN')
                & (df[column_MapCode] == 'AT')
            ]
        else:
            df_focus = df.loc[
                (df[column_AreaName] == 'AT BZN')
                & (df[column_MapCode] == 'AT')
            ]
    
    #elif country_code == 'AT1':
    #   df_focus = df.loc[
    #        (df[column_AreaName] == 'Austria')
    #       & (df[column_MapCode] == 'AT')
    #    ]
            
    elif country_code == 'FR':
        df_focus = df.loc[
            (df[column_AreaName] == 'FR BZN')
            & (df[column_MapCode] == 'FR')
        ]
        
    elif country_code == 'CH':
        df_focus = df.loc[
            (df[column_AreaName] == 'CH BZN')
            & (df[column_MapCode] == 'CH')
        ]
        
    elif country_code == 'ES':
        df_focus = df.loc[
            (df[column_AreaName] == 'ES BZN')
            & (df[column_MapCode] == 'ES')
        ]
        
    elif country_code == 'BE':
        df_focus = df.loc[
            (df[column_AreaName] == 'BE BZN')
            & (df[column_MapCode] == 'BE')
        ]
        
    elif country_code == 'PL':
        df_focus = df.loc[
            (df[column_AreaName] == 'PL BZN')
            & (df[column_MapCode] == 'PL')
        ]
        
    elif country_code == 'NL':
        df_focus = df.loc[
            (df[column_AreaName] == 'NL BZN')
            & (df[column_MapCode] == 'NL')
        ]
        
    elif country_code == 'GB':
        df_focus = df.loc[
            (df[column_AreaName] == 'GB BZN')
            & (df[column_MapCode] == 'GB')
        ]
        
    elif country_code == 'IT':
        df_focus = df.loc[
            (df[column_AreaName] == 'IT CTY')
            & (df[column_MapCode] == 'IT')
        ]
        
    elif country_code == 'IT_North':
        df_focus = df.loc[
            (df[column_AreaName] == 'IT-North BZN')
            & (df[column_MapCode] == 'IT_North')
        ]
    #-------------------------------------------------------------------------------------
    else:
        df_focus = df.loc[
            (df[column_MapCode] == country_code)
        ]
    
    return df_focus