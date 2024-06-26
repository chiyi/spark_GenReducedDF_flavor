import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession

object Interface {
 def usage(): Unit = {
  println("usage: select_date_HHMM(20240620_0000) output_filename nhours");
  val spark = SparkSession.builder.getOrCreate();
  spark.stop();
  sys.exit(-1);
 }

 def chk_para(args: Array[String]): Boolean = {
  val fmt = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
  if(args.length < 3 || args(1).length < 3 || args(2).length < 3) false
  else {
   try {
    val dt = fmt.parse(args(1))
    val yf = DateTimeFormatter.ofPattern("yyyy")
    if (yf.format(dt).toInt < 2000) false else true
   } catch {case _: Throwable => false}
  }
 }
}
