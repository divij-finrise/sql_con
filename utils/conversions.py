import pandas as pd
import os

def NSE_conversion(df_nse):
    if df_nse is None:
        return None
    try:
        df_src = pd.read_csv(f"{os.getcwd()}/utils/NSE-SGX.CSV", low_memory=False)
    except Exception as e:
        print("\n[Error] in (utils.utils, NSE_conversion) msg: ",str(e))

    df = df_src[['Exchange','Symbol','Security ID','SecurityType',
                'Expiry/Series','OptionType','StrikePrice','Multiplier',
                'Divider','BaseCurrency', 'Product ID'] ]

    df_conv = df.rename(columns={'Exchange':'exchange',
                    'Symbol':'symbol',
                    'Security ID': 'scripcode',
                    'SecurityType':'securitytype',
                    'Expiry/Series':'expirydate',
                    'OptionType':'opttype',
                    'StrikePrice':'strikeprice',
                    'Multiplier':'multiplier',
                    'Divider':'divider',
                    'BaseCurrency': 'basecurrency',
                    'Product ID':'productid'})

    df_merged = df_nse.merge(df_conv, on='scripcode', how='left')
    df_merged.drop(['exchange_x', 'opttype_x', 'expirydate_x',
                     'strikeprice_x', 'securitytype_x'], axis=1, inplace=True)
    df_merged = df_merged.rename(columns={
                    'exchange_y':'exchange',
                    'opttype_y':'opttype',
                    'expirydate_y':'expirydate',
                    'strikeprice_y':'strikeprice',
                    'securitytype_y':'securitytype'})
    df_merged = df_merged[
        ['inid','sqldatetime','datetime','tradenum','ordernum','userid',
        'terminalid','ctclid','algoid','accountcode','membercode',
        'scripcode','exchange','opttype','expirydate','strikeprice',
        'scripdescription','buysellflag','tradeqty','tradeprice','maintradeid',
        'securitytype','referencetext','pendingqty','customid','sender',
        'symbol', 'multiplier', 'divider','basecurrency','productid']
    ]
    return(df_merged)

def MCX_conversion(df_mcx):
    try:
        df_src = pd.read_csv(f"{os.getcwd()}/utils/MCX-CME-DGCX.CSV", low_memory=False)
    except Exception as e:
        print("\n[Error] in (utils.utils, MCX_conversion) msg: ",str(e))

    df = df_src[['Exchange','Symbol','Security ID','SecurityType',
                'Expiry/Series','OptionType','StrikePrice','Multiplier',
                'Divider','BaseCurrency', 'Product ID'] ]

    df_conv = df.rename(columns={'Exchange':'exchange',
                    'Symbol':'symbol',
                    'Security ID': 'scripcode',
                    'SecurityType':'securitytype',
                    'Expiry/Series':'expirydate',
                    'OptionType':'opttype',
                    'StrikePrice':'strikeprice',
                    'Multiplier':'multiplier',
                    'Divider':'divider',
                    'BaseCurrency': 'basecurrency',
                    'Product ID':'productid'})

    df_merged = df_mcx.merge(df_conv, on='scripcode', how='left')
    df_merged.drop(['exchange_x', 'opttype_x', 'expirydate_x',
                     'strikeprice_x', 'securitytype_x'], axis=1, inplace=True)
    df_merged = df_merged.rename(columns={
                    'exchange_y':'exchange',
                    'opttype_y':'opttype',
                    'expirydate_y':'expirydate',
                    'strikeprice_y':'strikeprice',
                    'securitytype_y':'securitytype'})
    df_merged = df_merged[
        ['inid','sqldatetime','datetime','tradenum','ordernum','userid',
        'terminalid','ctclid','algoid','accountcode','membercode',
        'scripcode','exchange','opttype','expirydate','strikeprice',
        'scripdescription','buysellflag','tradeqty','tradeprice','maintradeid',
        'securitytype','referencetext','pendingqty','customid','sender',
        'symbol', 'multiplier', 'divider','basecurrency','productid']
    ]
    return(df_merged)