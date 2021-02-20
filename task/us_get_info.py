# -*- coding:utf-8 -*-


import os
import sys

path = os.path.dirname(__file__) + os.sep + '..' + os.sep
sys.path.append(path)

from tools.util import *
from tools.mydb import *


# def us_total_cap(x):
#     if isinstance(x, str) and x.endswith('B'):
#         return float(x[1:-1]) * 10
#     elif isinstance(x, str) and x.endswith('M'):
#         return float(x[1:-1]) / 100
#     else:
#         return None


def us_total_cap(x):
    if isinstance(x, str) and len(x) > 0:
        return float(x)/100000000
    else:
        return None


def get_us_symbols_from_nasdaq_api():
    import requests
    dfs = []
    for market in ["NASDAQ", "NYSE", "AMEX"]:
        headers = {
            'authority': 'api.nasdaq.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'zh-CN,zh;q=0.9',
        }

        params = (
            ('exchange', market),
            ('download', 'true'),
        )

        response = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
        js = response.json()
        df = pd.json_normalize(js["data"]["rows"])
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    return df



def get_us_info():
    start = datetime.now()

    info_table = 'us_stocks_info'
    # 更新 标普500 权重股
    spx_columns = ['code', 'name', 'is_spx', 'sp_sector']
    mydb.upsert_table(info_table, spx_columns, us.get_spx())
    spx2_columns = ['code', 'name', 'spx_weight']
    mydb.upsert_table(info_table, spx2_columns, us.get_spx2())
    # 更新 纳斯达克100 权重股
    ndx_columns = ['code', 'name', 'is_ndx', 'ndx_weight']
    mydb.upsert_table(info_table, ndx_columns, us.get_ndx())
    # 更新 道琼斯 权重股
    dji_columns = ['code', 'name', 'is_dji', 'dji_weight']
    mydb.upsert_table(info_table, dji_columns, us.get_dji())

    # 全美股市场股票
    symbols = get_us_symbols_from_nasdaq_api()
    if symbols is not None and symbols.empty==False:
        columns = ['code', 'name', 'sector', 'industry', 'total_cap']
        symbols.rename(columns={'symbol': 'code', 'marketCap': 'total_cap'},
                    inplace=True)
        symbols = symbols[columns].set_index(['code']).drop_duplicates().reset_index()
        symbols.total_cap = symbols.total_cap.map(us_total_cap)
        mydb.upsert_table(info_table, columns, symbols)

    end = datetime.now()
    print('Download Data use {}'.format(end - start))
