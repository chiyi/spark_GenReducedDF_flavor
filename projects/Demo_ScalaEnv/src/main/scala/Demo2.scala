import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.SparkSession


object Demo2 {
 def main(args: Array[String]): Unit = {

  println("args=" + args.mkString(", "));
  if(args(0)=="demo2") demo2(args) else usage();

 }

 def demo2(args: Array[String]): Unit = {
  if(!chk_para(args)) usage();

  val spark = SparkSession.builder.appName("Demo2").getOrCreate();

  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
  val map_schema1d = read_schema1d();
  val test_key = "A_info.obj_x.value";
  println(Array(test_key , map_schema1d(test_key)).mkString(", "));

  spark.stop();
  println("DONE: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
 }



 def chk_para(args: Array[String]): Boolean = {
  val date_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  if(args.length<3 || args(1).length<3 || args(2).length<3) false
  else {
   try {
    val dt_obj = date_format.parse(args(1))
    val year_fmt = DateTimeFormatter.ofPattern("yyyy")
    if (year_fmt.format(dt_obj).toInt<2000) false else true
   } catch {case _: Throwable => false}
  }
 }

 def usage(): Unit = {
  println("usage: select_date_HHMM(20240620_0000) output_filename");
  sys.exit(-1);
 }

 def read_schema1d(): Map[String, String] = {
  val inp_file = "projects/Demo_ScalaEnv/table/Table_demo2.tsv";
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
}
