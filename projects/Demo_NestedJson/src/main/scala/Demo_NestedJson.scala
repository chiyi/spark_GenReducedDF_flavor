import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, ArrayType}
import org.apache.spark.sql.functions.col

import GetPath.{DataName, GLOBAL_DATAPATH_FORMAT, getpath_hourlylist}
import Interface.{usage, chk_para}


object Demo_NestedJson {
 def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder.getOrCreate();
  println("args=" + args.mkString(", "));
  if(args(0)=="Demo_NestedJson") demo_nestedjson(args) else usage();
  spark.stop();
 }

 def demo_nestedjson(args: Array[String]): DataFrame = {
  if(!chk_para(args)) usage();

  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
  val spark = SparkSession.builder.getOrCreate();
  val dth_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  val dt_proc = LocalDateTime.from(dth_format.parse(args(1)));
  var out_file = args(2);
  val nhours = args(3).toInt;
  val enum_dataname = DataName.OpenRTBNativeBidrequest;
  var inp_files = getpath_hourlylist(GLOBAL_DATAPATH_FORMAT(enum_dataname), dt_proc, 0, nhours);
  println(inp_files.mkString(", "));
  println(out_file);

  val schema = StructType(Seq(
   StructField("timestamp", LongType, true),
   StructField("BidRequest", StructType(Seq(
    StructField("id", StringType, true),
    StructField("site", StructType(Seq(
     StructField("page", StringType, true),
     StructField("domain", StringType, true),
     StructField("name", StringType, true)
    )), true),
    StructField("imp", ArrayType(StructType(Seq(
     //StructField("id", StringType, true),
     //StructField("tagid", StringType, true),
     StructField("bidfloor", DoubleType, true),
     StructField("bidfloorcur", StringType, true),
     StructField("native", StructType(Seq(
      StructField("request", StringType, true)
      //StructField("ver", StringType, true),
      //StructField("api", ArrayType(IntegerType()), true),
      //StructField("battr", ArrayType(IntegerType()), true)
     )), true)
    )), true))
   )), true)
  ));

  //val df0: DataFrame = spark.read.parquet(inp_files: _*);
  val df0: DataFrame = spark.read.schema(schema).parquet(inp_files: _*);
  //return df0

  var df = (df0.select("BidRequest.site", "BidRequest.imp")
               .selectExpr("site.domain as site_domain",
                           "explode(imp) as imp"
                          )
               .selectExpr("site_domain",
                           "imp.bidfloor", "imp.bidfloorcur",
                           "get_json_object(imp.native.request, '$.assets') as asset",
                           // "imp.native.request"    //debug
                          )
               //.where("site_domain='aaa.com.tw'")
           )
  //return df

  val schema_json = ArrayType(StructType(Seq(
   StructField("id", IntegerType, true),
   StructField("required", IntegerType, true),
   StructField("title", StructType(Seq(
    StructField("len", IntegerType, true)
   )), true),
   StructField("img", StructType(Seq(
    StructField("wmin", IntegerType, true),
    StructField("hmin", IntegerType, true),
    StructField("type", IntegerType, true)
   )), true),
   StructField("data", StructType(Seq(
    StructField("type", IntegerType, true),
    StructField("len", IntegerType, true)
   )), true)
  )));
     
  df = (df.selectExpr("site_domain",
                      "from_json(asset, '%s') as assets" format schema_json.catalogString)
          .selectExpr("site_domain", "filter(assets, x -> x.required==1 and x.img is not NULL)[0].img as asset_img")
          //.selectExpr("explode(filter(assets, x -> x.data is not NULL)) as asset_data")
       )
  //return df

  //var gdf = (df.select("asset_data.required", "asset_data.data.type")
  //             .groupBy("required", "type")
  //             .count()
  //             .orderBy(col("count").desc)
  //          )

  df = df.select("site_domain", "asset_img.wmin", "asset_img.hmin")
  //var gdf = (df.select("wmin", "hmin")
  //             .groupBy("wmin", "hmin")
  //             .count()
  //             .orderBy(col("count").desc)
  //          )
  var gdf = (df.select("site_domain", "wmin", "hmin")
               .groupBy("site_domain", "wmin", "hmin")
               .count()
               .orderBy(col("count").desc)
            )

  gdf = gdf.repartition(1).orderBy(col("count").desc)

  ( gdf.write.format("csv")
             .option("sep", "\t")
             .option("header", "true")
             .save(out_file)
  )


  println("DONE: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

  gdf
 }
}
