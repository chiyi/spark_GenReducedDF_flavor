import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, BooleanType, BinaryType, ArrayType}
import scala.io.Source

import GetPath.getpath_hourlylist
import Interface.{usage, chk_para}


object GenTable {
 def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder.getOrCreate();
  println("args=" + args.mkString(", "));
  if(args(0)=="schema1d") gen_schema1d(args) else usage();
  spark.stop();
 }

 def gen_schema1d(args: Array[String]): Unit = {
  if(!chk_para(args)) usage();

  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
  val spark = SparkSession.builder.getOrCreate();

  val dth_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  val dt_proc = LocalDateTime.from(dth_format.parse(args(1)));
  var out_file = args(2);    //"projects/DataRichness/table/Table_NonEmptyCols_fake.tsv"
  val nhours = args(3).toInt;    //"3"
  var path_format = args(4);    //"'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'"
  var inp_files = getpath_hourlylist(path_format, dt_proc, 0, nhours);
  println(inp_files.mkString(", "));
  println(out_file);

  var df = spark.read.parquet(inp_files: _*);
  var arr_schema = df.schema.toArray;
  var outstr = Array[String]();
  outstr = recur_1dcols(arr_schema);
  outstr = chk_emptycols(df, outstr);
  write_outfile(out_file, outstr);
  print_rows(outstr.asInstanceOf[Array[Any]]);

  println("DONE: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
 }




 def recur_1dcols(
  fields: Array[StructField],
  col_prefix: String = "",
  ddl_fmt: String = "%s",
  b_dig: Boolean = false):
  Array[String] = 
 {
  fields.flatMap { field =>
   field match {
    case StructField(name, dataType, _, _) if dataType.isInstanceOf[StructType] => {
     val next_prefix = s"${col_prefix}${name}.";
     val next_ddl = gen_ddlstr_by(ddl_fmt, f"${name} STRUCT<%%s>", name, "STRUCT<%s>", b_dig);
       recur_1dcols(dataType.asInstanceOf[StructType].fields, next_prefix, next_ddl, true)
    }
    case StructField(name, dataType, _, _) if dataType.isInstanceOf[ArrayType] => {
     val element = dataType.asInstanceOf[ArrayType].elementType;
     val next_prefix = s"${col_prefix}${name}[]";
     if (element.isInstanceOf[StructType])
     {
      val next_ddl = gen_ddlstr_by(ddl_fmt, f"${name} ARRAY<%%s>", name, "ARRAY<STRUCT<%s>>", b_dig);
      recur_1dcols(element.asInstanceOf[StructType].toArray, next_prefix+".", next_ddl, true)
     }
     else
     {
      val colname1d = next_prefix;
      val ddlstr = gen_ddlstr_by(ddl_fmt, field.toDDL, name, dataType.sql, b_dig);
      Seq(s"${ddlstr}\t${colname1d}\t${element.sql}")
     }
    }
    case _ => {
     val colname1d = s"${col_prefix}${field.name}";
     val ddlstr = gen_ddlstr_by(ddl_fmt, field.toDDL, field.name, field.dataType.sql, b_dig)
     Seq(s"${ddlstr}\t${colname1d}\t${field.dataType.sql}")
    }
   }
  }
 }

 def gen_ddlstr_by(ddl_fmt: String, ddlstr: String, name: String, value: String, b_dig: Boolean = false): String = {
  if (b_dig)
   ddl_fmt format (f"${name}: ${value}")
  else
   ddl_fmt format (f"${ddlstr}");
 }

 def chk_emptycols(df: DataFrame, cols_1d: Array[String]): Array[String] = {
  cols_1d.map { icol =>
   val name1d = icol.split("\t")(1);
   val chk_list = name1d.split("\\[\\]");
   var colname = name1d.replace("[]", "");
   var tmpdf = df.selectExpr(f"${colname} as tmpcol");
   for (idx <- 1 until chk_list.size)
   {
    tmpdf = tmpdf.selectExpr("explode(tmpcol) as tmpcol");
   }
   val tmprow = tmpdf.where("tmpcol is not NULL").take(1);
   val is_nonempty = tmprow.size > 0;
 
   f"${icol}\t${is_nonempty}"
  }.toArray
 }

 def write_outfile(filename: String, str: Array[String]): Unit = {
  val outfile = new java.io.File(filename);
  val writer = new java.io.PrintWriter(outfile);
  val header = "schema_DDL\tschema1d_name\tschema1d_type\tis_nonempty";
  var outstr = header +: str;
  outstr.foreach(writer.println);
  writer.close();
 }
 
 def print_rows(rows: Array[Any]): Unit = {
  rows.foreach {
   case x => println(x)
  }
 }
}


