# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import datetime, pandas, sys

from interface import chk_para

def load_df(inpfn):
    res = pandas.read_csv(inpfn, sep=',', encoding='utf-16')
    res.columns = [icol.replace('<', '').replace('>', '') for icol in res.columns]
    return res

def gen_inpfn(dt_proc):
    folder = 'projects/Demo_CreateData/data'
    fmt = 'USDCHFXXXM5_%Y_%m_%d_%HH%M.csv'
    filename = dt_proc.strftime(fmt)
    res = f'{folder}/{filename}'
    return res

def gen_outfn(dt_proc):
    folder = 'projects/Demo_CreateData/data/USDCHF_M5'
    fmt = '%Y/%m/%d/%HH/USDCHFXXXM5_%Y_%m_%d_%HH%M.parquet'
    filename = dt_proc.strftime(fmt)
    res = f'{folder}/{filename}'
    return res

def create_dfparq(inp_para):
    print('BEGIN:', datetime.datetime.now())
    chk_para(inp_para)

    dt_proc = datetime.datetime.strptime(inp_para[1], '%Y%m%d_%H%M')
    inpfn = gen_inpfn(dt_proc)
    outfn = gen_outfn(dt_proc)
    print(inpfn, outfn)
    spark = SparkSession.builder.getOrCreate()
    df = load_df(inpfn)
    df = spark.createDataFrame(df)
    df.write.parquet(outfn)

    print('END:', datetime.datetime.now())
    return df

if __name__ == '__main__':
    print('sys.argv=', sys.argv)
    if 'create_dfparq.py' in sys.argv[0]:
        create_dfparq(sys.argv)

