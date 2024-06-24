import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Demo {
 def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder
    .appName("Demo")
    .getOrCreate();
  println("args=" + args.mkString(", "));
  demo1(args);
  //spark.stop();
 }

 def demo1(inp_para: Array[String]): Unit = {
  println("BEGIN: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
  chk_para(inp_para);
  println("DONE: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
 }




 def chk_para(inp_para: Array[String]): Unit = {
  val dth_format = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  val yyyy = DateTimeFormatter.ofPattern("yyyy");

  if(inp_para.length<3) usage();

  try {
   val dt_obj = dth_format.parse(inp_para(1));
   val year = yyyy.format(dt_obj).toInt;
   if(year<2000) usage();
   if(inp_para(2).length<3) usage();
  } catch {
   case _: Throwable => usage()
  }
 }

 def usage(): Unit = {
  println("usage: select_date_HHMM(20240620_0000) output_filename");
  println("");
  sys.exit(-1);
 }
}
