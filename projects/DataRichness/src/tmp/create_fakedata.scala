import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, DoubleType, StringType, BooleanType, BinaryType, ArrayType}
import scala.collection.JavaConverters._

val schema = StructType(Seq(
 StructField("timestamp", LongType, true),
 StructField("fake_data", StructType(Seq(
  StructField("fake_id", BinaryType, true),
  StructField("fake_field1", StringType, true),
  StructField("fake_field2_empty", IntegerType, true),
  StructField("fake_field3", ArrayType(IntegerType, true)),
  StructField("fake_field4_empty", StringType, true),
  StructField("fake_field5", StructType(Seq(
   StructField("field5_br1", ArrayType(StructType(Seq(
    StructField("field5_br1_1", StringType, true),
    StructField("field5_br1_2", ArrayType(StringType, true)),
    StructField("field5_br1_3_empty", ArrayType(IntegerType, true)),
   )), true))
  )), true),
  StructField("fake_field6", IntegerType, true),
  StructField("fake_field7_empty", StructType(Seq(
   StructField("fake_field7_br1", IntegerType, true),
   StructField("fake_field7_br2", StringType, true),
   StructField("fake_field7_br3", StringType, true),
  )), true),
  StructField("fake_field8", StructType(Seq(
   StructField("field8_br1_1", StringType, true),
   StructField("field8_br1_2", StructType(Seq(
    StructField("field8_br1_2_1", ArrayType(StructType(Seq(
     StructField("field8_br1_2_1_1", StructType(Seq(
      StructField("field8_br1_2_1_1_1", LongType, true),
      StructField("field8_br1_2_1_1_2", BooleanType, true),
      StructField("field8_br1_2_1_1_3", StringType, true),
     )), true),
     StructField("field8_br1_2_1_2", StructType(Seq(
      StructField("field8_br1_2_1_2_1", ArrayType(LongType, true))
     )), true),
    )), true)),
   )), true),
  )), true),
 )), true)
));

def gen_fakefield3(ts0: Long): Array[Int] = {
 val length = (ts0 % 7).toInt;
 (0 until length).map{ idx =>
  ((ts0 / 10000 + idx * 149) % 29).toInt;
 }.toArray
}

def gen_fakefield5(ts0: Long): Row = {
 val cand_cats = Seq(
  Row("cat1", Seq("cat1111", "cat1111", "cat1111"), null),
  Row("cat2", Seq("dog"), null),
  Row("cat3", Seq(), null),
  Row("cat4", Seq("sea", "see", "c", "seem"), null),
  Row("others", null, null),
 )

 val length = (ts0 % 4).toInt;
 val field5_br1 = (0 until length).map { idx =>
  cand_cats(((ts0 / 60000 + idx * 31) % 5).toInt);
 }.toArray;

 Row(field5_br1)
}

def gen_fakefield8(ts0: Long): Row = {
 val cands = Seq(
  Row(Row(1000000L, true, "st"), null),
  Row(Row(1111111L, false, ""), Row(Seq(1000000L, 1111111L))),
  Row(Row(0L, false, "pp"), Row(Seq(0L, 9999999L, 888888L, 77777777L))),
  Row(Row(22222222L, true, "xx"), Row(Seq(123456789L))),
  Row(null, Row(Seq(111111111L))),
 )

 val field8_br1_2_1 = (0 until 5).map { idx =>
  cands(((ts0 % 79 + idx * 11) % 5).toInt);
 }.toArray;

 Row(
  "",                   //field8_br1_1
  Row(field8_br1_2_1)   //field8_br1_2
 )
}

def gen_fakerow(ts0: Long): Row = {
 val record_id = f"recid_$ts0".getBytes("UTF-8");
 val field3 = gen_fakefield3(ts0);
 val field5 = gen_fakefield5(ts0);
 val field8 = gen_fakefield8(ts0);
 var fakedata = Row(
  record_id,     // fake_id
  "val1",        // field1
  null,          // field2
  field3,        // field3
  null,          // field4
  field5,        // field5
  3,             // field6
  null,          // field7
  field8         // field8
 );
 var res = Row(ts0, fakedata);
 res
}

def gen_fakedf(): DataFrame = {
 val tmprow1 = gen_fakerow(1718895700);
 val tmprow2 = gen_fakerow(1718895701);
 val tmprow3 = gen_fakerow(1718895702);
 val tmprow4 = gen_fakerow(1718895733);
 val tmpdata = Seq(tmprow1, tmprow2, tmprow3, tmprow4).toList;
 val resdf = spark.createDataFrame(tmpdata.asJava, schema);

 resdf
}

def make_fakedata(): Unit = {
 val path_fmt = "'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'";
 val dt_fmt = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
 val dthours = Seq("20240620_0800", "20240620_0900", "20240620_1000");
 val rows = dthours.flatMap { dthm =>
  val dt_proc = LocalDateTime.from(dt_fmt.parse(dthm));
  val ts_begin = dt_proc.toEpochSecond(ZoneOffset.UTC);
  var tmprows = (0 until 2000).map { ts_i =>
   var ts0 = ts_begin + ts_i;
   gen_fakerow(ts0)
  };
  val resdf = spark.createDataFrame(tmprows.toList.asJava, schema);
  val outpath = DateTimeFormatter.ofPattern(path_fmt).format(dt_proc).toString;
  resdf.write.parquet(outpath);
  println(outpath);
  tmprows
 }// end for dt_hour

 spark.createDataFrame(rows.toList.asJava, schema)
}

