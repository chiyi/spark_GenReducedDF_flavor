import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, ArrayType}
import org.apache.spark.sql.functions.udf
import GetPath.getpath_hourlylist




// code for developing the `RecurCount` object implementation

def recur_count_elements(value: Any, format: String): String = {
 var res = "0";

 val arr_format = format.split('.');
 val format_head = arr_format.head;
 val b_head_list = format_head.endsWith("[]");
 val col_head = format_head.replace("[]", "");
 val format_tail = arr_format.tail.mkString(".");
 value match {
  case null => "0"
  case row: Row if row == null => "0"
  case row: Row if arr_format.size == 1 && row.get(0) == null => "0"
  case row: Row if arr_format.size == 1 && !b_head_list && row.get(0) != null => "1"
  case row: Row if arr_format.size == 1 && b_head_list && row.get(0) != null =>
   row.asInstanceOf[Row].getAs[Seq[Any]](col_head).size.toString
  case row: Row if !b_head_list => {
   val rowvalue = row.asInstanceOf[Row].getAs[Row](col_head);
   recur_count_elements(rowvalue, format_tail)
  }
  case row: Row if b_head_list => {
   val rowlist = row.getAs[Seq[Row]](col_head);
   rowlist.map(recur_count_elements(_, format_tail)).mkString("")
  }
  case _ => throw new RuntimeException("Unexpected value/type")
 }
}
def UDF_recur_count_elements = udf((value: Row, format: String) => recur_count_elements(value.asInstanceOf[Any], format.split('.').drop(1).mkString(".")));
spark.udf.register("UDF_recur_count_elements", UDF_recur_count_elements);


def func_chk(value: Any, format: String): String = {
 //if (value.isInstanceOf[Row])
 // value.asInstanceOf[Row].toString
 if (value.isInstanceOf[Array[Byte]])
  value.asInstanceOf[Array[Byte]].toSeq.toString
 else
  "others"
}
def UDF_func_chk = udf((value: Any, format: String) => func_chk(value.asInstanceOf[Any], format))
spark.udf.register("func_chk", UDF_func_chk);


val dth_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
val dt_proc = LocalDateTime.from(dth_format.parse("20240620_0800"));
val nhours = 3;
val path_format = "'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'";
var inp_files = getpath_hourlylist(path_format, dt_proc, 0, nhours);


val schema1d_ddl = "fake_data STRUCT<fake_field5: STRUCT<field5_br1: ARRAY<STRUCT<field5_br1_2: ARRAY<STRING>>>>>";
val schema1d_col = "fake_data.fake_field5.field5_br1[].field5_br1_2[]";


val schema: StructType = StructType.fromDDL(schema1d_ddl);
val df0: DataFrame = spark.read.schema(schema).parquet(inp_files: _*);
var df: DataFrame = df0.select("fake_data");
val tail_colname: String = df.columns.last;

df = (df.where(s"$tail_colname is not NULL")
        .selectExpr(s"UDF_recur_count_elements($tail_colname , '$schema1d_col') as size_info", s"$tail_colname")
//        //.where("size_info>0")
     )

//var df_test = df.selectExpr(f"func_chk(${schema1d_col}, '${schema1d_ddl}') as tmp")
