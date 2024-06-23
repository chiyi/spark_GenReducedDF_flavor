## BasicAggregation
for developing basic aggregation tasks

### Fake Data
There are some examples for creating fake data in structured parquet-file form via RDD in [src/create_fakedata.py](BasicAggregation/src/create_fakedata.py).  
If we don't have real data, we may need to write some fake content to populate the dataset. Once it's done, process it.

```
# in pyspark shell/interpreter
# set outpath with respect to the dataset in 'projects/common/get_path.py'.
# example: 'projects/BasicAggregation/data/BidRequest/Google/2024/06/19/14H/GoogleBidRequest_2024_06_19_14H10.parquet''
exec(open('projects/BasicAggregation/src/create_fakedata.py', 'rb').read())
```

### Check Google Bid Response Feedback
The processing code [check_googlefdbk.py](BasicAggregation/src/check_googlefdbk.py) read the dataset `DataName.GoogleBidRequest` for the specified hour, and read the dataset `DataName.GoogleFeedback` with an expanding time window. 
The feedback information is joined with the bid request data for further aggregation. Finally, the aggregated result is saved into a pandas CSV-file.

```
# in repository spark_GenReducedDF_flavor directory
./run_pyspark.sh
# in pyspark shell/interpreter
In [1]: exec(open('projects/BasicAggregation/src/check_googlefdbk.py', 'rb').read())
sys.argv= ['/usr/local/bin/ipython3']

In [2]: resdf = check_googlefdbk(['', '20240620_1010', 'projects/BasicAggregation/res/test.out'])
BEGIN: 2024-06-23 14:17:41.325805
output_filename projects/BasicAggregation/res/test.out
projects/BasicAggregation/data/BidRequest/Google/2024/06/20/10H/GoogleBidRequest_2024_06_20_10H*.parquet
['projects/BasicAggregation/data/BidFeedback/Google/2024/06/20/09H/GoogleFeedback_2024_06_20_09H*.parquet', 'projects/BasicAggregation/data/BidFeedback/Google/2024/06/20/10H/GoogleFeedback_2024_06_20_10H*.parquet', 'projects/BasicAggregation/data/BidFeedback/Google/2024/06/20/11H/GoogleFeedback_2024_06_20_11H*.parquet']
DONE: 2024-06-23 14:17:45.588367
```

```
# or run the defined task
projects/BasicAggregation/tasks/run_check_googlefdbk.sh
```

### Aggregate ConvLog for OrderValue Relatives
The processing code [aggconv_ordervalue.py](projects/BasicAggregation/src/aggconv_ordervalue.py) read the dataset `DataName.JoinlogConv` for aggregating on the specified groups.  
The aggregated result is saved into a pandas CSV-file.

```
# in pyspark shell/interpreter
In [1]: exec(open('projects/BasicAggregation/src/aggconv_ordervalue.py', 'rb').read())
sys.argv= ['/usr/local/bin/ipython3']

In [2]: resdf = aggconv_ordervalue(['', '20240620_1400', 'projects/BasicAggregation/res/test.out'])
BEGIN: 2024-06-23 14:26:18.012548
['projects/BasicAggregation/data/ImpConv/2024/06/20/14H/Join_ImpConv_2024_06_20_14H.parquet']   projects/BasicAggregation/res/test.out
DONE: 2024-06-23 14:26:20.896460
```

```
# the defined task
projects/BasicAggregation/tasks/run_aggconv_ordervalue.sh
```

### Transform Data for Preparing Binned Likelihood Calculation
The processing code [trsfm_binned_likelihood.py](projects/BasicAggregation/src/trsfm_binned_likelihood.py) read the dataset `DataName.JoinlogClk` for performing various complex mappings on selected columns as a demonstration. 
Each row of data is categorized into multiple groups for further binned likelihood calculation.

```
# in pyspark shell/interpreter
# if we don't have supported hdfs, set output_filename from argument
In [1]: exec(open('projects/BasicAggregation/src/trsfm_binned_likelihood.py', 'rb').read())
sys.argv= ['/usr/local/bin/ipython3']

In [2]: resdf = trsfm_binned_likelihood(['', '20240620_1500', 'projects/BasicAggregation/res/test.out'])
BEGIN: 2024-06-23 14:42:22.491259
projects/BasicAggregation/data/ImpClk/2024/06/20/15H/Join_ImpClk_2024_06_20_15H*.parquet _ projects/BasicAggregation/data/GroupAna/Trsfm_binnedLd_GroupingAna_2024062015utc.parquet
```

```
# the defined task
projects/BasicAggregation/tasks/run_trsfm_binned_likelihood.sh
```
