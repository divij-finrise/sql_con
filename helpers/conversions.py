import pandas as pd
import os

def NSE_conversion(df_nse):
    df_src = pd.read_csv("/home/techteam/divij/sql_conn/helpers/NSE-SGX.CSV")

    df = df_src[['Exchange','Symbol','Security ID','SecurityType',
                'Expiry/Series','OptionType','StrikePrice','Multiplier',
                'Divider','BaseCurrency', 'Product ID'] ]

    df.rename(columns={'Exchange':'exchange',
                    'Symbol':'symbol',
                    'Security ID': 'scripcode',
                    'SecurityType':'securitytype',
                    'Expiry/Series':'expirydate',
                    'OptionType':'opttype',
                    'StrikePrice':'strikeprice',
                    'Multiplier':'multiplier',
                    'Divider':'divider',
                    'BaseCurrency': 'basecurrency',
                    'Product ID':'productid'}, inplace=True)
    return(df)
    #df_merged = pd.concat([df_nse.set_index('scripcode'),df.set_index('scripcode')], axis=1, join='inner').reset_index()


def MCX_conversion(df_mcx):
    df_src = pd.read_csv("/home/techteam/divij/sql_conn/helpers/MCX-CME-DGCX.CSV")

    df = df_src[['Exchange','Symbol','Security ID','SecurityType',
                'Expiry/Series','OptionType','StrikePrice','Multiplier',
                'Divider','BaseCurrency', 'Product ID'] ]

    df.rename(columns={'Exchange':'exchange',
                    'Symbol':'symbol',
                    'Security ID': 'scripcode',
                    'SecurityType':'securitytype',
                    'Expiry/Series':'expirydate',
                    'OptionType':'opttype',
                    'StrikePrice':'strikeprice',
                    'Multiplier':'multiplier',
                    'Divider':'divider',
                    'BaseCurrency': 'basecurrency',
                    'Product ID':'productid'}, inplace=True)
    return(df)
    #df_merged = pd.concat([df_mcx.set_index('scripcode'),df.set_index('scripcode')], axis=1, join='inner').reset_index()
 
 #def clean(df):
    