# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, LongType, BooleanType, BinaryType
import datetime, sys, pandas

from interface import chk_para
from cust_pyspark import add_column_byexpression
from get_path import DataName, GLOBAL_DATAPATH_FORMAT, getpath_hourlylist


def getdf_feedback(dt_proc):
    file_fmt = GLOBAL_DATAPATH_FORMAT[DataName.GoogleFeedback]
    list_inppath = getpath_hourlylist(file_fmt, dt_proc, -1, 2)
    print(list_inppath)

    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField('timestamp', LongType(), True),
        StructField('bid_response_feedback', StructType([
            StructField('request_id', BinaryType(), True),
            StructField('creative_status_code', IntegerType(), True),
            StructField('cpm_micros', LongType(), True),
            StructField('minimum_bid_to_win', LongType(), True),
            StructField('buyer_creative_id', StringType(), True),
        ]), True),
    ])
    df0 = spark.read.schema(schema).parquet(*list_inppath)
    df = df0.selectExpr("timestamp as ts_feedback", "bid_response_feedback")
    return df

def join_feedback(inpdf_bid, inpdf_feedback):
    cond_join = [
        #inpdf_bid.ts_bid/1e6-300 <= inpdf_feedback.ts_feedback/1e6,
        #inpdf_feedback.ts_feedback/1e6 <= inpdf_bid.ts_bid/1e6+300,
        inpdf_bid.req_id == inpdf_feedback.bid_response_feedback.request_id
    ]
    df_join = inpdf_bid.join(inpdf_feedback, cond_join, 'left')
    return df_join

def readdf_req(inp_path):
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField('timestamp', LongType(), True),
        StructField('is_bid', BooleanType(), True),
        StructField('BidRequest', StructType([
            StructField('id', BinaryType(), True),
            StructField('google_query_id', StringType(), True),
            StructField('publisher_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('mobile', StructType([
                StructField('is_app', BooleanType(), True),
                StructField('app_id', StringType(), True),
            ]), True),
            StructField('adslot', ArrayType(StructType([
                StructField('native_ad_template', ArrayType(StructType([
                    StructField('required_fields', LongType(), True),
                ]),True)),
            ]),True)),
        ]), True),
    ])
    df0 = spark.read.schema(schema).parquet(inp_path)
    return df0

def gendf_bidbasic(dt_proc):
    enum_dataname = DataName.GoogleBidRequest
    file_fmt = GLOBAL_DATAPATH_FORMAT[enum_dataname].replace('%M', '*')
    inp_path = dt_proc.strftime(file_fmt)
    print(inp_path)
    df0 = readdf_req(inp_path)
    df = (df0.where("is_bid")
             .select('timestamp', 'BidRequest')
         )
    return df


# ==== procedures ====
# 
def check_googlefdbk(inp_para):
    print('BEGIN:', datetime.datetime.now())
    chk_para(inp_para)

    dt_proc = datetime.datetime.strptime(inp_para[1], '%Y%m%d_%H%M')
    output_filename = inp_para[2]
    print('output_filename', output_filename)

    df0 = gendf_bidbasic(dt_proc)
    df = (df0.selectExpr(
        'timestamp as ts_bid',
        'BidRequest.id as req_id',
        'BidRequest.mobile.is_app')
    )
    df.cache()
    #return df
   
    df_feedback = getdf_feedback(dt_proc)
    df_feedback.cache()
    #return df_feedback
    df_joinfb = join_feedback(df, df_feedback)
    df_joinfb.cache()
    #return df_joinfb

    df_joinfb = add_column_byexpression(df_joinfb, "IF(bid_response_feedback is NULL, 1, 0) as no_feedback")
    df_joinfb = add_column_byexpression(df_joinfb, 'bid_response_feedback.creative_status_code as status')
    df_joinfb = add_column_byexpression(df_joinfb, "IF(status in (1,79,80), 1, 0) as sel_status")
    df_joinfb = add_column_byexpression(df_joinfb, "IF(sel_status=1, cast(status as string), 'others') as creative_status_code")
    df_joinfb = add_column_byexpression(df_joinfb, "IF(bid_response_feedback.cpm_micros=0 and bid_response_feedback.minimum_bid_to_win=0, 1, 0) as is_EmptyWP")
    df_joinfb = (df_joinfb.selectExpr(
        'is_app',
        'no_feedback', 'creative_status_code', 'is_EmptyWP',
        #'bid_response_feedback.cpm_micros as WP_2ndPriceAuction',
        #'bid_response_feedback.minimum_bid_to_win as WP_1stPriceAuction')
        )
    )
    df_joinfb.cache()
    #return df_joinfb

    sel_groups = [
        'creative_status_code',
        'no_feedback', 'is_EmptyWP',
        #'bin_10log10WP_2ndPriceAuction', 'bin_10log10WP_1stPriceAuction', 'bin_10log10BP'
    ]

    GDF = df_joinfb.groupBy(*sel_groups).count().orderBy(*sel_groups)
    #return GDF

    GDF = GDF.toPandas()
    GDF.to_csv(output_filename, sep='\t')
    print('DONE:', datetime.datetime.now())
    return GDF

if __name__ == '__main__':
    print('sys.argv=', sys.argv)
    if 'check_googlefdbk.py' in sys.argv[0]:
        check_googlefdbk(sys.argv)

