import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, BooleanType, BinaryType, ArrayType}
import scala.io.Source



def read_fakedf(): DataFrame = {
 val inp_path = "projects/DataRichness/data/Fake/2024/06/20/*H/*.parquet";
 val spark = SparkSession.builder.getOrCreate();
 spark.read.parquet(inp_path);
}

def print_rows(rows: Array[Any]): Unit = {
 rows.foreach {
  case x => println(x)
 }
}

def recur_1dcols(
 cols_begin: Array[String],
 fields: Array[StructField],
 col_prefix: String = ""):
 Array[String] = 
{
 fields.flatMap { field =>
  field match {
   case field: StructField if field.dataType.isInstanceOf[StructType] =>
    recur_1dcols(cols_begin, field.dataType.asInstanceOf[StructType].fields, s"${col_prefix}${field.name}.")
   case field: StructField if field.dataType.isInstanceOf[ArrayType] => {
    val element = field.dataType.asInstanceOf[ArrayType].elementType;
    if (element.isInstanceOf[StructType])
     recur_1dcols(cols_begin, element.asInstanceOf[StructType].toArray, s"${col_prefix}${field.name}[].");
    else
     Seq(s"${col_prefix}${field.name}[]\t${element.sql}")
   }
   case _ => Seq(s"${col_prefix}${field.name}\t${field.dataType.sql}")
  }
 }
}

def chk_empty(df: DataFrame, cols_1d: Array[String]): Array[String] = {
 cols_1d.map { icol =>
  val name1d = icol.split("\t")(0);
  var chk_list = name1d.split("\\[\\]");
  var colname = name1d.replace("[]", "");
  var tmpdf = df.selectExpr(f"${colname} as tmpx");
  for (idx <- 1 until chk_list.size)
  {
   tmpdf = tmpdf.selectExpr("explode(tmpx) as tmpx");
  }
  val tmprow = tmpdf.where("tmpx is not NULL").take(1);
  val is_nonempty = if(tmprow.size > 0) 1 else 0;

  f"${icol}\t${is_nonempty}"
 }.toArray
}

def gen_explode1d_nonemptycols(): Unit = {
 val df = read_fakedf();
 var arr_schema = df.schema.toArray;
 var outstr = Array[String]();
 outstr = recur_1dcols(outstr, arr_schema);
 outstr = chk_empty(df, outstr);

 val outfile = new java.io.File("projects/DataRichness/table/Table_NonEmptyCols_fake.tsv");
 val writer = new java.io.PrintWriter(outfile);
 val header = "schema1d_name\tschema1d_type\tis_nonempty";
 outstr = header +: outstr;
 outstr.foreach(writer.println);
 writer.close();

 print_rows(outstr.asInstanceOf[Array[Any]]);
}


