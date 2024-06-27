# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType, IntegerType, ArrayType, LongType, BooleanType, BinaryType, TimestampType
import datetime, math, os, sys, time, pandas
import numpy
import scipy
import scipy.special
import scipy.special.cython_special

from interface import chk_para
from cust_pyspark import add_column_byexpression
from pyspark.sql import functions as F


def Cal_loglikelihood_binomial(num_successes, probability_success, total_trials):
    # Ensure input values are valid
    assert num_successes >= 0 and total_trials >= 0 and probability_success >= 0. and probability_success <= 1.

    # Convert input values to numpy float64
    num_successes = numpy.float64(num_successes)
    total_trials = numpy.float64(total_trials)
    probclip_left = numpy.clip(numpy.float64(probability_success), 1e-15, 1)
    probclip_right = numpy.clip(numpy.float64(probability_success), 0, 1 - 1e-15)

    # Calculate log likelihood
    res = 0.
    if num_successes > 0.:
       res += num_successes * numpy.log(probclip_left)
    if total_trials - num_successes > 0.:
       res += (total_trials - num_successes) * numpy.log1p(-probclip_right)
    if num_successes > 0. and total_trials - num_successes > 0.:
        res += (
            scipy.special.cython_special.loggamma(total_trials + 1.)
            - scipy.special.cython_special.loggamma(num_successes + 1.)
            - scipy.special.cython_special.loggamma(total_trials - num_successes + 1.)
        )
    return float(res)

def Cal_loglikelihood_ratio_binomial(num_successes, probability_success, total_trials):
    res = Cal_loglikelihood_binomial(num_successes, probability_success, total_trials)

    log_max_ll = 0.
    total_trials = numpy.float64(total_trials)
    num_successes = numpy.float64(num_successes)
    f64_mean_pread = numpy.float64(probability_success)
    if num_successes > 0 and (total_trials - num_successes) > 0:
        log_max_ll = (
            scipy.special.cython_special.loggamma(total_trials + 1.)
            - scipy.special.cython_special.loggamma(num_successes + 1.)
            - scipy.special.cython_special.loggamma(total_trials - num_successes + 1.)
            + num_successes * numpy.log(num_successes / total_trials) + (total_trials - num_successes) * numpy.log1p(-num_successes / total_trials)
        )
    return float(res - log_max_ll)




def aggbinlh_llh(inp_para):
    print("BEGIN: " + str(datetime.datetime.now()))
    chk_para(inp_para)

    dt_proc = datetime.datetime.strptime(inp_para[1], '%Y%m%d_%H%M')
    output_filename = inp_para[2]
    inp_path = 'hdfs:/user/kaiyi/Ref/GroupAna/Trsfm_binnedLd_GroupingAna_{}utc.parquet'.format(dt_proc.strftime('%Y%m%d%H'))
    #inp_path = 'projects/BasicAggregation/data/GroupAna/Trsfm_binnedLd_GroupingAna_{}utc.parquet'.format(dt_proc.strftime('%Y%m%d%H'))
    print(inp_path, output_filename)
    spark = SparkSession.builder.getOrCreate()
    spark.catalog.registerFunction("Cal_loglikelihood_binomial", Cal_loglikelihood_binomial, DoubleType())
    spark.catalog.registerFunction("Cal_loglikelihood_ratio_binomial", Cal_loglikelihood_ratio_binomial, DoubleType())

    df0 = spark.read.parquet(inp_path)
    #return df0
    # if no data
    df0 = (df0.selectExpr("explode(groups) as group",
                          "12345 as Nimp",
                          "cast(0.123 as double) as sumPred",
                          "is_vldclk as sumLabel",
                          "cast(1.111 as double) as sumlogloss",
                          "cost as sumCost")
          )

    df = df0.select('group', 'Nimp', 'sumPred', 'sumLabel', 'sumlogloss', 'sumCost')
    GDF = (df.groupBy('group')
             .agg(F.sum('Nimp').alias('Nimp'),
                  F.sum('sumPred').alias('sumPred'),
                  F.sum('sumLabel').alias('sumLabel'),
                  F.sum('sumlogloss').alias('sumlogloss'),
                  F.sum('sumCost').alias('sumCost'))
          )
    GDF.cache()
    print(GDF.count())
    GDF = add_column_byexpression(GDF, "Cal_loglikelihood_binomial(sumLabel, sumPred/Nimp, Nimp) as loglikelihood")
    GDF = add_column_byexpression(GDF, "Cal_loglikelihood_ratio_binomial(sumLabel, sumPred/Nimp, Nimp) as loglikelihoodratio")
    return GDF

    GDF = (GDF.groupBy()
              .agg(F.count('group').alias('n_groups'),
                   F.sum('loglikelihood').alias('loglikelihood'),
                   F.sum('loglikelihoodratio').alias('loglikelihoodratio'),
                   F.sum('sumlogloss').alias('sumlogloss'),
                   F.sum('sumCost').alias('sumCost'))
          )
    GDF.cache()
    GDF = GDF.toPandas()

    GDF.insert(0, 'dateutc_24hr', inp_para[1])
    GDF.to_csv(output_filename, sep='\t')
    return GDF


if __name__ == '__main__':
    print("sys.argv=",sys.argv)
    if 'aggbinlh_llh.py' in sys.argv[0]:
        aggbinlh_llh(sys.argv)

