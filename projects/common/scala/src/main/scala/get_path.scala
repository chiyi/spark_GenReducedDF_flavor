import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.HOURS

object GetPath {
 object DataName extends Enumeration {
  type DataName = Value
  val GoogleBidRequest = 1;
  val GoogleFeedback = 2;
  val OpenRTBNativeBidrequest = 3;
  val JoinlogClk = 4;
  val JoinlogConv = 5;
 };

 val GLOBAL_DATAPATH_FORMAT = Map(
  //DataName.GoogleBidRequest -> "'hdfs:/record/BidRequest/Google/'yyyy/MM/dd/HH'H'/'GoogleBidRequest_'yyyy_MM_dd_HH'H'mm'.parquet'",
  DataName.GoogleBidRequest -> "'projects/BasicAggregation/data/BidRequest/Google/'yyyy/MM/dd/HH'H'/'GoogleBidRequest_'yyyy_MM_dd_HH'H'mm'.parquet'",
  //DataName.GoogleFeedback -> "'hdfs:/skim/BidFeedback/Google/'yyyy/MM/dd/HH'H'/'GoogleFeedback_'yyyy_MM_dd_HH'H'mm'.parquet'",
  DataName.GoogleFeedback -> "'projects/BasicAggregation/data/BidFeedback/Google/'yyyy/MM/dd/HH'H'/'GoogleFeedback_'yyyy_MM_dd_HH'H'mm'.parquet'",
  //DataName.OpenRTBNativeBidrequest -> "'hdfs:/record/BidRequest/OpenRTB/Media9/'yyyy/MM/dd/HH'H'/'OpenRTBMedia9BidRequest_'yyyy_MM_dd_HH'H'mm'.parquet'",
  DataName.OpenRTBNativeBidrequest -> "'projects/Demo_NestedJson/data/OpenRTBNativeBidrequest/media_9/'yyyy/MM/dd/HH'H'/'Media9BidRequest_'yyyy_MM_dd_HH'H'mm'.parquet'",
  //DataName.JoinlogClk -> "'hdfs:/join/learn/ImpClk/'yyyy/MM/dd/HH'H'/'Join_ImpClk_'yyyy_MM_dd_HH'H'mm'.parquet'",
  DataName.JoinlogClk -> "'projects/BasicAggregation/data/ImpClk/'yyyy/MM/dd/HH'H'/'Join_ImpClk_'yyyy_MM_dd_HH'H'mm'.parquet'",
  //DataName.JoinlogConv -> "'hdfs:/join/learn/ImpConv/'yyyy/MM/dd/HH'H'/'Join_ImpConv_'yyyy_MM_dd_HH'H'mm'.parquet'",
  DataName.JoinlogConv -> "'projects/BasicAggregation/data/ImpConv/'yyyy/MM/dd/HH'H'/'Join_ImpConv_'yyyy_MM_dd_HH'H.parquet'"
 );

 def getpath_hourlylist(path_fmt: String, dt_proc: LocalDateTime, hour_begin: Int = 0, hour_endexc: Int = 24): Seq[String] = {
  val fmt = path_fmt.replace("mm", "*");
  (hour_begin until hour_endexc).map {
   ihour =>
   val dt_tmp = dt_proc.plusHours(ihour);
   DateTimeFormatter.ofPattern(fmt).format(dt_tmp).toString
  }.toSeq
 }
}
