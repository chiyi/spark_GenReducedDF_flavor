import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
import org.apache.spark.sql.functions.udf

//def func_chk(value: Any, format: String): String = {
// value.asInstanceOf[Row].getAs[Row](format).toString
//}

//def recur_count_elements(value: Any, format: String): String = {
// //return value.asInstanceOf[Row].schema.toString
// val arr_format = format.split('.');
// value match {
//  case null => ""
//  case row: Row if arr_format.size == 1 => row.size.toString
//  case row: Row => {
//   val key_1st = arr_format(0).replace("[]", "");
//   val format_tail = arr_format.drop(1).mkString(".");
//   val b_list_key1st = arr_format(0).endsWith("[]");
//   
//   var res = "";
//   if (b_list_key1st)
//   {
//    println(arr_format(0) + " is list");
//    val rowlist = row.asInstanceOf[Row].getAs[Seq[Row]](key_1st);
//    for(iele <- rowlist)
//    {
//     //var iele_row = iele.getAs[Row][key_1st];
//     println(" * calling recur_elements for list");
//     println(iele.schema.toString + " , format= " + format_tail);
//     res = res + " " + recur_count_elements(iele, format_tail);
//     ////println(iele.schema.toString + " , format= " + format_tail);
//     ////res = res + " " + recur_count_elements(iele, format_tail);
//    }
//    //rowlist.toString
//    res
//   }
//   else
//   {
//    println(arr_format(0) + "." + arr_format(1) + "isn't list");
//    val rowvalue = row.asInstanceOf[Row].getAs[Row](key_1st);
//    println(" * calling next recur_elements");
//    println(rowvalue.schema.toString + " , format= " + format_tail);
//    recur_count_elements(rowvalue, format_tail)
//   }
//  }
//  case _ => ""
// }
//}
////def UDF_recur_count_elements = udf((value: Row, format: String) => recur_count_elements(value, format), StringType);


object TestEnv {
 def main(args: Array[String]): Unit = {

  println("args=" + args.mkString(", "));
  if(args(0)=="test") test(args) else usage();

 }

 def recur_count_elements(value: Any, format: String): Int = {
  var res = 0;
 
  val arr_format = format.split('.');
  value match {
   case null => 0
   case row: Row if arr_format.size == 1 => if (arr_format(0).endsWith("[]")) row.size else 1
   case row: Row => {
    var res = 0;
 
    val key_1st = arr_format(0).replace("[]", "");
    val format_tail = arr_format.drop(1).mkString(".");
    val b_list_key1st = arr_format(0).endsWith("[]");
    if (b_list_key1st)
    {
     val rowlist = row.asInstanceOf[Row].getAs[Seq[Row]](key_1st);
     for(iele <- rowlist)
     {
      res = res + recur_count_elements(iele, format_tail);
     }
     res
    }
    else
    {
     val rowvalue = row.asInstanceOf[Row].getAs[Row](key_1st);
     recur_count_elements(rowvalue, format_tail)
    }
   }
   case _ => 0
  }
 }
 def UDF_recur_count_elements = udf((value: Row, format: String) => recur_count_elements(value, format.split('.').drop(1).mkString(".")), IntegerType);


 def test(args: Array[String]): Unit = {
 //def test(args: Array[String]): DataFrame = {
  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

  if(!isValid(args)) usage();

  val spark = SparkSession.builder.getOrCreate();

  spark.udf.register("UDF_recur_count_elements", UDF_recur_count_elements);

  val inp_files = Array(REMOVED);
  val inp_files_nobid = Array(REMOVED);
  val out_filename = args(2);
  println(inp_files.mkString(", "));
  println(inp_files_nobid.mkString(", "));
  println(out_filename);

  val map_schema1d = read_schema1d();
  val test_key = "RTBRequest.adslot[].native_ad_template[].app_icon_height";
  println(Array(test_key , map_schema1d(test_key)).mkString(", "));

  val test_struct = "struct<RTBRequest:struct<adslot:array<struct<native_ad_template:array<struct<app_icon_height:int>>>>>>";
  //val test_struct = "struct<RTBRequest:struct<url:string>>";
  val n_data = count_nonempty(spark, test_key, test_struct, inp_files, inp_files_nobid);


  spark.stop();
  println("END: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
  println("DONE");

  //n_data
 }



 def isValid(args: Array[String]): Boolean = {
  val dateFormat = DateTimeFormatter.ofPattern("yyyyMMddHH");
  if(args.length<3 || args(1).length<3 || args(2).length<3) false
  else {
   try {
    val dt_obj = dateFormat.parse(args(1))
    val yearFormat = DateTimeFormatter.ofPattern("yyyy")
    if (yearFormat.format(dt_obj).toInt<2000) false else true
   } catch {case _: Throwable => false}
  }
 }

 def usage(): Unit = {
  println("usage: select_dateH(2023010100) output_filename");
  sys.exit(-1);
 }

 def read_schema1d(): Map[String, String] = {
  val inp_file = "test_scala/table/Table_NonEmptyCols_GoogleRTB.tsv";
  val lines = Source.fromFile(inp_file).getLines().toList;
  val header = lines.head.split("\t").map(_.trim);

  val idx_schema1d_name = header.indexOf("schema1d_name");
  val idx_schema1d_type = header.indexOf("schema1d_type");
  val idx_is_nonempty = header.indexOf("is_nonempty");

  val res = lines.tail
    .map(_.split("\t").map(_.trim))
    .filter(_(idx_is_nonempty).toBoolean)
    .map(columns => (columns(idx_schema1d_name), columns(idx_schema1d_type)))
    .toMap

  res
 }

 def count_nonempty(spark: SparkSession, schema1d_key: String, str_datatype: String, list_path: Seq[String], list_path_nobid: Seq[String]): Long = {
 //def count_nonempty(spark: SparkSession, schema1d_key: String, str_datatype: String, list_path: Seq[String], list_path_nobid: Seq[String]): DataFrame = {
  val schema_strbidlog: String = s"struct<RTBLogInfo:$str_datatype>";
  val schema_strnobidlog: String = s"struct<RTBNoBidLogInfo:$str_datatype>";
  val schema_bidlog: DataType = DataType.fromDDL(schema_strbidlog);
  val schema_nobidlog: DataType = DataType.fromDDL(schema_strnobidlog);
  val df0: DataFrame = spark.read.schema(schema_bidlog.asInstanceOf[StructType]).parquet(list_path: _*);
  val df0_nobid: DataFrame = spark.read.schema(schema_nobidlog.asInstanceOf[StructType]).parquet(list_path_nobid: _*);
  var df: DataFrame = df0.select("RTBLogInfo.RTBRequest");
  df = df.union(df0_nobid.select("RTBNoBidLogInfo.RTBRequest"));

  val tail_colname: String = df.columns.last;
  df = (df.where(s"$tail_colname is not NULL")
          .selectExpr(s"UDF_recur_count_elements($tail_colname , '$schema1d_key') as size_info")
          .where("size_info>0")
       )
  val res: Long = df.count();
  //val res = 0;

  println(schema1d_key + " " + res.toString);

  //df
  return res
 }
}
