## Demo NestedJson
When working with big data, it's not uncommon to encounter parquet columns that contain JSON strings. Parsing these JSON strings in PySpark can be slow, especially when dealing with large datasets.
This Scala demonstration showcases an efficient approach to analyze such data.

By leveraging Scala's native JSON parsing capabilities and Spark's DataFrame API, we can process JSON strings in parquet columns more efficiently, resulting in faster data analysis and reduced processing times.

### Developing in spark shell
* reference for creating fake data in [create_fakedata.scala](src/tmp/create_fakedata.scala)  
* modify the implementation in [Demo_NestedJson.scala](src/main/scala/Demo_NestedJson.scala) 
```
# in spark shell
scala> :load /opt/spark/work-dir/projects/Demo_NestedJson/src/main/scala/Demo_NestedJson.scala
Loading /opt/spark/work-dir/projects/Demo_NestedJson/src/main/scala/Demo_NestedJson.scala...
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, ArrayType}
import org.apache.spark.sql.functions.col
import GetPath.{DataName, GLOBAL_DATAPATH_FORMAT, getpath_hourlylist}
import Interface.{usage, chk_para}
defined object Demo_NestedJson

scala> var resdf = Demo_NestedJson.demo_nestedjson(Array("", "20240620_1511", "test.out", "1"))
BEGIN: 2024-06-26 13:39:20
projects/Demo_NestedJson/data/OpenRTBNativeBidrequest/media_9/2024/06/20/15H/Media9BidRequest_2024_06_20_15H*.parquet
test.out
DONE: 2024-06-26 13:39:23
resdf: org.apache.spark.sql.DataFrame = [site_domain: string, wmin: int ... 2 more fields]

scala> resdf.show()
+-----------+----+----+-----+
|site_domain|wmin|hmin|count|
+-----------+----+----+-----+
|    aaa.com|  50|  50|    3|
+-----------+----+----+-----+
```

### Build and Submit tasks
```
# in container
cd projects/Demo_NestedJson/
sbt compile
sbt package
ls target/scala-2.12/demo_nestedjson_2.12-0.0.jar 
```
```
# exit the container
# execute defined task
projects/Demo_NestedJson/tasks/run_demo_nestedjson.sh
```
