# spark_GenReducedDF_flavor
It's a pyspark specialization in aggregating data from a specified time range to generate the reduced output into pandas dataframe.

* build docker image from [spark:3.5.1-scala2.12-java17-python3-ubuntu](Dockerfile#L1) with my specialization in `Dockerfile`
  ```
  ./build.sh
  ```
* run spark via various entries for the following purposes in the projects.
  ```
  ./run_container.sh
  or ./run_pyspark.sh
  or ./run_spark.sh
  or ./run_sparksql.sh
  ```

### example
* [projects/BasicAggregation](projects/BasicAggregation)
