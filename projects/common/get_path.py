import datetime
from enum import Enum

class DataName(Enum):
    GoogleBidRequest = 1
    GoogleFeedback = 2
    JoinlogClk = 3
    JoinlogConv = 4


def getpath_hourlylist(path_fmt, dt_proc, hour_begin=0, hour_endexc=24):
    res = list()
    fmt = path_fmt.replace('%M', '*')
    for ihour in range(hour_begin, hour_endexc):
        dt_tmp = dt_proc + ihour * datetime.timedelta(hours=1)
        res.append(dt_tmp.strftime(fmt))
    return res

GLOBAL_DATAPATH_FORMAT = {
    #DataName.GoogleBidRequest: 'hdfs:/record/BidRequest/Google/%Y/%m/%d/%HH/GoogleBidRequest_%Y_%m_%d_%HH%M.parquet',
    DataName.GoogleBidRequest: 'projects/BasicAggregation/data/BidRequest/Google/%Y/%m/%d/%HH/GoogleBidRequest_%Y_%m_%d_%HH%M.parquet',
    #DataName.GoogleFeedback: 'hdfs:/skim/BidFeedback/Google/%Y/%m/%d/%HH/GoogleFeedback_%Y_%m_%d_%HH%M.parquet'
    DataName.GoogleFeedback: 'projects/BasicAggregation/data/BidFeedback/Google/%Y/%m/%d/%HH/GoogleFeedback_%Y_%m_%d_%HH%M.parquet',
    #DataName.JoinlogClk: 'hdfs:/join/learn/ImpClk/%Y/%m/%d/%HH/Join_ImpClk_%Y_%m_%d_%HH%M.parquet',
    #DataName.JoinlogConv: 'hdfs:/join/learn/ImpConv/%Y/%m/%d/%HH/Join_ImpConv_%Y_%m_%d_%HH%M.parquet',
    DataName.JoinlogConv: 'projects/BasicAggregation/data/ImpConv/%Y/%m/%d/%HH/Join_ImpConv_%Y_%m_%d_%HH.parquet'
}

