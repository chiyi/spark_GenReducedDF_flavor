from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, MapType, IntegerType, ArrayType, LongType, BooleanType, BinaryType, TimestampType


local_struct_convlog = StructType([
    StructField('timestamp', LongType(), True),
    StructField('conversion_id', StringType(), True),
    StructField('buy_order', StringType(), True),
    StructField('value', FloatType(), True),
])
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
    #StructField('convloglist_from_click', ArrayType(local_struct_convlog, True)),
])

def gen_fakerow_impclk(ts0, sel_req=1):
    tmpreq = Row(BidRequest=Row(mobile=None))
    if sel_req == 1:
        tmpreq = Row(
            BidRequest=Row(
                url='aaa.bbb.ccc/123.txt',
                user_agent='iphone xxx',
                mobile=Row(
                    is_app=True,
                    app_id='test_app'
                )
            )
        )
    elif sel_req == 2:
        tmpreq = Row(
            BidRequest=Row(
                url='aaa.ccc.com',
                user_agent='ipad',
                mobile=Row(
                    is_app=False,
                    app_id=''
                )
            )
        )
    tmpclklist = []
    tmpclk = Row(
        timestamp = ts0 + 3664,
        is_valid = True
    )
    tmpclklist.append(tmpclk)
    res = Row(
        record_id='recid_202406201455123456_ijkl1234',
        implog=Row(
            timestamp=ts0,
            implog_info=Row(
                media_id=1,
                advertiser_id='adv_lm123',
                cost_per_mille=100,
                rtblog=Row(tmpreq)
            )
        ),
        clickloglist=tmpclklist
    )
    return res

def gen_fakerow_impconv(ts0, sel_req=1, sel_conv=[]):
    tmpreq = Row(BidRequest=Row(mobile=None))
    if sel_req == 1:
        tmpreq = Row(
            BidRequest=Row(
                mobile=Row(
                    is_app=False,
                    app_id=''
                )
            )
        )
    elif sel_req == 2:
        tmpreq = Row(
            BidRequest=Row(
                mobile=Row(
                    is_app=True,
                    app_id='test_app'
                )
            )
        )
    tmpconvlist = []
    if 1 in sel_conv:
        tmpconv = Row(
            timestamp = ts0 + 3664,
            conversion_id = 'buy',
            buy_order = 123456789,
            value = float(1000.0)
        )
        tmpconvlist.append(tmpconv)
    if 2 in sel_conv:
        tmpconv = Row(
            timestamp = ts0 + 14211,
            conversion_id = 'buy',
            buy_order = 133333333,
            value = float(900.0)
        )
        tmpconvlist.append(tmpconv)
    res = Row(
        record_id='recid_202406201455123456_ijkl1234',
        implog=Row(
            timestamp=ts0,
            implog_info=Row(
                media_id=1,
                cost_per_mille=100,
                rtblog=Row(tmpreq)
            )
        ),
        convloglist_from_click=tmpconvlist
    )
    return res


#tmprdd1 = gen_fakerow_impconv(1718895336, 0, [])
#tmprdd2 = gen_fakerow_impconv(1718895700, 1, [1, 2])
#tmprdd3 = gen_fakerow_impconv(1718899990, 2, [2])

tmprdd1 = gen_fakerow_impclk(1718895700, 1)
tmprdd2 = gen_fakerow_impclk(1718895990, 2)

df = spark.createDataFrame([tmprdd1, tmprdd2], schema)
#df = spark.createDataFrame([tmprdd1, tmprdd2, tmprdd3], schema)
#df.write.parquet(outpath)
