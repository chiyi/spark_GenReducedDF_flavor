# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, MapType, IntegerType, ArrayType, LongType, BooleanType, BinaryType, TimestampType
import datetime, math, os, sys, time, pandas, re

from interface import chk_para
from cust_pyspark import add_column_byexpression
from get_path import DataName, GLOBAL_DATAPATH_FORMAT, getpath_hourlylist
from pyspark.sql import functions as F




def aggconv_ordervalue(inp_para):
    print('BEGIN:', datetime.datetime.now())
    chk_para(inp_para)

    dt_proc = datetime.datetime.strptime(inp_para[1], '%Y%m%d_%H%M')
    output_filename = inp_para[2]
    enum_dataname = DataName.JoinlogConv
    inp_path = getpath_hourlylist(GLOBAL_DATAPATH_FORMAT[enum_dataname], dt_proc, 0, 1)
    print(inp_path, ' ', output_filename)
    spark = SparkSession.builder.getOrCreate()

    local_struct_convlog = StructType([
        StructField('timestamp', LongType(), True),
        #StructField('conversion_id', StringType(), True),
        StructField('buy_order', StringType(), True),
        StructField('value', FloatType(), True),
    ])

    schema = StructType([
        StructField('record_id', StringType(), True),
        StructField('implog', StructType([
            StructField('timestamp', LongType(), True),
            StructField('implog_info', StructType([
                StructField('media_id', IntegerType(), True),
                StructField('cost_per_mille', LongType(), True),
                StructField('rtblog', StructType([
                    StructField('rtblog_info', StructType([
                        StructField('BidRequest', StructType([
                            StructField('mobile', StructType([
                                StructField('is_app', BooleanType(), True),
                                StructField('app_id', StringType(), True),
                            ]), True),
                        ]), True),
                    ]), True),
                ]), True),
            ]), True),
        ]), True),
        StructField('convloglist_from_click', ArrayType(local_struct_convlog, True)),
    ])
    df0 = spark.read.schema(schema).parquet(*inp_path)
    #return df0

    df = (df0.select('record_id', 'implog.implog_info', 'convloglist_from_click')
             .where("size(convloglist_from_click)>0")
             .where("implog_info.media_id in (1,3,5,7,9)")
             .selectExpr('record_id',
                         'implog_info.media_id',
                         'implog_info.rtblog.rtblog_info.BidRequest',
                         "filter(convloglist_from_click, x -> x.timestamp>0) as convloglist")
             .where("size(convloglist)>0")
             .selectExpr('record_id', 'media_id',
                         "map(True, 1, False, 0)[BidRequest.mobile.is_app] as is_app",
                         "array_distinct(convloglist) as convloglist")
             .selectExpr('record_id', 'media_id', 'is_app',
                         "explode(convloglist) as convlog")
             .selectExpr('media_id', 'is_app', "convlog.value as order_value")
         )
    #df.cache()
    #return df

    sel_groups = ['media_id', 'is_app']
    GDF = (df.groupBy(*sel_groups)
             .agg(F.count('media_id').alias('Nconv'),
                  F.sum('order_value').alias('order_value')
                 )
          ).orderBy(*sel_groups)
    GDF.cache()
    #return GDF
    GDF = GDF.toPandas()

    GDF.insert(0, 'dateutc_24hr', inp_para[1])
    GDF.to_csv( output_filename, sep='\t')
    print('DONE:', datetime.datetime.now())
    return GDF

if __name__ == '__main__':
    print("sys.argv=",sys.argv)
    if 'aggconv_ordervalue.py' in sys.argv[0]:
        aggconv_ordervalue(sys.argv)

