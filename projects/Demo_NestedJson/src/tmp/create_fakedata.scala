import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, BooleanType, BinaryType, ArrayType}
import scala.collection.JavaConverters._

val schema = StructType(Seq(
 StructField("timestamp", LongType, true),
 StructField("is_bid", BooleanType, true),
 StructField("media_id", IntegerType, true),
 StructField("BidRequest", StructType(Seq(
  StructField("id", StringType, true),
  StructField("site", StructType(Seq(
   StructField("page", StringType, true),
   StructField("domain", StringType, true),
   StructField("name", StringType, true)
  )), true),
  StructField("imp", ArrayType(StructType(Seq(
   StructField("id", StringType, true),
   StructField("tagid", StringType, true),
   StructField("bidfloor", DoubleType, true),
   StructField("bidfloorcur", StringType, true),
   StructField("native", StructType(Seq(
     StructField("request", StringType, true)
   )), true)
  )), true))
 )), true)
));

def gen_fakerow_req(ts0: Long): Row = {
 //https://www.iab.com/wp-content/uploads/2018/03/OpenRTB-Native-Ads-Specification-Final-1.2.pdf
 //6.3
 //val jsonstr = """{"ver":"2018_native_1.2", "context": 2, "contextsubtype":20, "plcmttype":11, "plcmtcnt":1, "aurlsupport":1, "durlsupport":1, "assets":[{"id":123,"required":1,"title":{"len":140}},{"id":128,"required":0,"img":{"wmin":836,"hmin":627,"type":3}},{"id":124,"required":1,"img":{"wmin":50,"hmin":50,"type":1}},{"id":126,"required":1,"data":{"type":1,"len":25}},{"id":127,"required":1,"data":{"type":2,"len":140}}]}""";
 val jsonstr = """
 {
  "ver":"2018_native_1.2",
  "context": 2,
  "contextsubtype":20,
  "plcmttype":11,
  "plcmtcnt":1,
  "aurlsupport":1,
  "durlsupport":1,
  "assets":[
   {"id":123,"required":1,"title":{"len":140}},
   {"id":128,"required":0,"img":{"wmin":836,"hmin":627,"type":3}},
   {"id":124,"required":1,"img":{"wmin":50,"hmin":50,"type":1}},
   {"id":126,"required":1,"data":{"type":1,"len":25}},
   {"id":127,"required":1,"data":{"type":2,"len":140}}
  ]
 }""";
 var res = Row(
  ts0,          // timestamp
  true,         // is_bid
  9,            // media_id
  Row(          // BidRequest
   "req_11111", //id
   Row(         // site
    "aaa.com/xxx",
    "aaa.com",
    "aaa"
   ),
   Array(Row(
    "id",
    "tagid_1111",
    0.10,
    "USD",
    Row(        // native
     jsonstr
    )
   ))
  )
 );
 res
}
 
val tmprow1 = gen_fakerow_req(1718895700)
val tmprow2 = gen_fakerow_req(1718895701)
val tmprow3 = gen_fakerow_req(1718895702)
val tmpdata = Seq(tmprow1, tmprow2, tmprow3).toList
val resdf = spark.createDataFrame(tmpdata.asJava, schema)
//:load projects/common/scala/src/main/scala/get_path.scala
//val dataname = GetPath.DataName.OpenRTBNativeBidrequest
//val sel_fmt = GetPath.GLOBAL_DATAPATH_FORMAT(dataname)
//val date_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")
//val dt_obj = LocalDateTime.from(date_format.parse("20240620_1500"))
//val outpath = DateTimeFormatter.ofPattern(sel_fmt).format(dt_obj).toString
//resdf.write.parquet(outpath)

