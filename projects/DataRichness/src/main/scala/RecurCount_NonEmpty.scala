import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
import org.apache.spark.sql.functions.udf

import GetPath.getpath_hourlylist
import Interface.{usage, chk_para}

object RecurCount {
 val spark = SparkSession.builder.appName("RecurCount").getOrCreate();
 def main(args: Array[String]): Unit = {
  println("args=" + args.mkString(", "));
  if(args(0)=="count_nonempty") recur_count(args) else usage();
  spark.stop();
 }

 def recur_count(args: Array[String]): Unit = {
  if(!chk_para(args)) usage();

  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

  val dth_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  val dt_proc = LocalDateTime.from(dth_format.parse(args(1)));
  val out_file = args(2);
  val nhours = args(3).toInt;
  val path_format = args(4);    //"'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'"
  val test_schema1d_col = args(5);    //"fake_data.fake_field5.field5_br1[].field5_br1_2[]"
  var inp_files = getpath_hourlylist(path_format, dt_proc, 0, nhours);
  println(inp_files.mkString(", "));
  println(out_file);

  var df = spark.read.parquet(inp_files: _*);
  df.show();

  val map_schema1d = read_schema1d();
  println("testing map_schema1d");
  val test_schema1d = map_schema1d(test_schema1d_col);
  println(Array(test_schema1d_col, test_schema1d(1), test_schema1d(0)).mkString(", "));
  val test_ddl = test_schema1d(0);
  val n_data = count_nonempty(spark, test_schema1d_col, test_ddl, inp_files);

  println("DONE: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
 }




 def recur_count_elements(value: Any, format: String): Int = {
  var res = 0;
 
  val arr_format = format.split('.');
  val format_head = arr_format.head;
  val b_head_list = format_head.endsWith("[]");
  val col_head = format_head.replace("[]", "");
  val format_tail = arr_format.tail.mkString(".");
  value match {
   case null => 0
   case row: Row if row == null => 0
   case row: Row if arr_format.size == 1 && row.get(0) == null => 0
   case row: Row if arr_format.size == 1 && !b_head_list && row.get(0) != null => 1
   case row: Row if arr_format.size == 1 && b_head_list && row.get(0) != null =>
    row.asInstanceOf[Row].getAs[Seq[Any]](col_head).size
   case row: Row if !b_head_list => {
    val rowvalue = row.asInstanceOf[Row].getAs[Row](col_head);
    recur_count_elements(rowvalue, format_tail)
   }
   case row: Row if b_head_list => {
    val rowlist = row.getAs[Seq[Row]](col_head);
    rowlist.map(recur_count_elements(_, format_tail)).sum
   }
   case _ => throw new RuntimeException("Unexpected value/type")
  }
 }
 def UDF_recur_count_elements = udf((value: Row, format: String) => recur_count_elements(value.asInstanceOf[Any], format.split('.').drop(1).mkString(".")));


 def read_schema1d(): Map[String, Array[String]] = {
  val inp_file = "projects/DataRichness/table/Table_NonEmptyCols_fake.tsv";
  val lines = Source.fromFile(inp_file).getLines().toList;
  val header = lines.head.split("\t").map(_.trim);
  
  val idx_schema_ddl = header.indexOf("schema_DDL");
  val idx_schema1d_name = header.indexOf("schema1d_name");
  val idx_schema1d_type = header.indexOf("schema1d_type");
  val idx_is_nonempty = header.indexOf("is_nonempty");

  val res = lines.tail
    .map(_.split("\t").map(_.trim))
    .filter(_(idx_is_nonempty).toBoolean)
    .map(columns => (columns(idx_schema1d_name), Array(columns(idx_schema_ddl), columns(idx_schema1d_type))))
    .toMap
  
  res
 }

 def count_nonempty(spark: SparkSession, schema1d_col: String, schema1d_ddl: String, list_path: Seq[String]): Long = {
  //spark.udf.register("func_chk", UDF_func_chk);
  spark.udf.register("UDF_recur_count_elements", UDF_recur_count_elements);

  val schema: DataType = DataType.fromDDL(schema1d_ddl);
  val df0: DataFrame = spark.read.schema(schema.asInstanceOf[StructType]).parquet(list_path: _*);
  var df: DataFrame = df0.select("fake_data");
  val tail_colname: String = df.columns.last;
  df = (df.where(s"$tail_colname is not NULL")
          .selectExpr(s"UDF_recur_count_elements($tail_colname , '$schema1d_col') as size_info",
                      tail_colname
                     )
       )
  df.show();
  df = df.where("size_info>0");
  val res: Long = df.count();

  // print the result or optionally write intermediate data in a specific format for further analysis
  println(f"RESULT: ${schema1d_col}, \t${res.toString}");
  
  return res
 }

 def func_chk(value: Any, format: String): String = {
  //if (value.isInstanceOf[Row])
  // value.asInstanceOf[Row].toString
  // value.schema.fields.head.name
  // value.json
  if (value.isInstanceOf[Array[Byte]])
   value.asInstanceOf[Array[Byte]].toSeq.toString
  else
   "others"
 }
 def UDF_func_chk = udf((value: Any, format: String) => func_chk(value.asInstanceOf[Any], format))
}
