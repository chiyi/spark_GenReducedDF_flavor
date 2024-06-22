# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, MapType, IntegerType, ArrayType, LongType, BooleanType, BinaryType, TimestampType, DecimalType
import datetime, math, os, sys, time, pandas

from interface import chk_para
from get_path import DataName, GLOBAL_DATAPATH_FORMAT
from pyspark.sql import functions as F

def Test_UDF(ua):
    res = 'Others'
    listkw = ['iphone', 'ipad', 'windows phone', 'android']
    ua = ua.lower()
    for ikw in listkw:
        if ikw in ua:
            res = 'CatA'
            break
    return res

def registerUDFS(spark):
    spark.catalog.registerFunction('Demo_Classify', Test_UDF, StringType())
    return spark




def trsfm_binned_likelihood(inp_para):
    print('BEGIN:', datetime.datetime.now())
    chk_para(inp_para)

    dt_proc = datetime.datetime.strptime(inp_para[1], '%Y%m%d_%H%M')
    output_filename = inp_para[2]
    enum_dataname = DataName.JoinlogClk
    inp_path = dt_proc.strftime(GLOBAL_DATAPATH_FORMAT[enum_dataname].replace('%M', '*'))
    output_filename = 'hdfs:/user/kaiyi/Ref/GroupAna/Trsfm_binnedLd_GroupingAna_{}utc.parquet'.format(dt_proc.strftime('%Y%m%d%H'))
    print(inp_path, '_', output_filename)
    spark = SparkSession.builder.getOrCreate()
    spark = registerUDFS(spark)

    local_struct_clicklog = StructType([
        StructField('timestamp', LongType(), True),
        StructField('is_valid', BooleanType(), True),
    ])
    schema = StructType([
        StructField('record_id', StringType(), True),
        StructField('implog', StructType([
            StructField('timestamp', LongType(), True),
            StructField('implog_info', StructType([
                StructField('media_id', IntegerType(), True),
                StructField('advertiser_id', StringType(), True),
                StructField('cost_per_mille', LongType(), True),
                StructField('rtblog', StructType([
                    StructField('timestamp', LongType(), True),
                    StructField('rtblog_info', StructType([
                        StructField('BidRequest', StructType([
                            StructField('url', StringType(), True),
                            StructField('user_agent', StringType(), True),
                            StructField('mobile', StructType([
                                StructField('is_app', BooleanType(), True),
                                StructField('app_id', StringType(), True),
                            ]), True),
                        ]), True),
                    ]), True),
                ]), True),
            ]), True),
        ]), True),
        StructField('clickloglist', ArrayType(local_struct_clicklog, True)),
    ])
    df0 = spark.read.schema(schema).parquet(inp_path)
    #return df0
    df = ( df0.select('record_id', 'implog.implog_info', 'clickloglist')
              .selectExpr('record_id',
                          'implog_info.media_id',
                          'implog_info.advertiser_id',
                          "implog_info.cost_per_mille/1.0e3 as cost",
                          'implog_info.rtblog.rtblog_info.BidRequest',
                          "implog_info.rtblog.timestamp as ts_req",
                          "IF(size(filter(clickloglist, x -> x.is_valid))>0, 1, 0) as is_vldclk",
                         )
              .selectExpr('record_id', 'media_id', 'advertiser_id',
                          'is_vldclk', 'cost', 'BidRequest', 'ts_req',
                          "parse_url(BidRequest.url, 'HOST') as domain",
                          "map(True, 'app', False, 'mobileweb')[BidRequest.mobile.is_app] as is_app",
                         )
              .where("media_id in (1,3,5,7,9)")
              .where("length(advertiser_id)>0")
              .selectExpr('record_id', 'media_id', 'cost',
                          "media_id as group_mediaid",
                          "advertiser_id as group_advid",
                          "domain as group_domain",
                          "cast(floor(log10(cost)) as int) as group_cost",
                          "BidRequest.mobile.app_id as group_appid",
                          "Demo_Classify(BidRequest.user_agent) as demo_cat",
                          "is_app as group_isapp",
                          'is_vldclk'
                         )
              .selectExpr('record_id', 'media_id', 'is_vldclk', 'cost',
                          "array( format_string('advid:%s', group_advid),"
                                 "format_string('mediaid:%d', group_mediaid),"
                                 "IF(group_domain is not NULL, format_string('domain:%s', group_domain), NULL),"
                                 "IF(group_cost is not NULL, format_string('cost_bin:%d', group_cost), NULL),"
                                 "IF(group_appid is not NULL, format_string('appid:%s', group_appid), NULL),"
                                 "format_string('isapp:%s', group_isapp)"
                               ") as groups"
                         )
         )
    df.write.parquet(output_filename)
    return df


if __name__ == '__main__':
    print("sys.argv=",sys.argv)
    if 'trsfm_binned_likelihood.py' in sys.argv[0]:
        trsfm_binned_likelihood(sys.argv)

