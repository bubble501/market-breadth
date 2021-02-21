# -*- coding:utf-8 -*-

import os
import sys
import click

path = os.path.dirname(__file__) + os.sep + '..' + os.sep
sys.path.append(path)

from us_get_daily import get_us_daily_ohlc
from us_get_info import get_us_info

from zh_get_daily import get_zh_daily_ohlc
from zh_get_info import get_zh_info
from task.tools.utils import analysis
from task.tools.mydb import mydb

@click.command()
@click.option('--market', '-m', default="zh", type=str,
        help="the market which will be used.")  
@click.option('--ignore', '-i', default="zh", type=str,
        help="ingore download.")  

def gen_market_breadth(market="zh", ignore="N"):
    config_dict = {}
    if market== 'zh':
        if ignore=="N":
            get_zh_info()
            get_zh_daily_ohlc()
        config_dict["zh_stocks_industries_d"] = "zh_stocks_industries"
        config_dict["zh_stocks_sector_sw_d"] = "zh_stocks_sw"

    elif market == "us":
        if ignore=="N":
            # get_us_info()
            get_us_daily_ohlc()
        config_dict["us_stocks_sector_d"] = "us_stocks_sector"

    for table, name in config_dict.items():
        df = mydb.read_from_sql(f"SELECT * FROM {table} ORDER BY date desc;")
        a_mb_name = path + 'data/' + name
        analysis.market_breadth(df, a_mb_name)


if __name__ == "__main__":
    gen_market_breadth()



