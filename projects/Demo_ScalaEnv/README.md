## Demo ScalaEnv
for developing in scala environment

### Developing in spark shell
modify the implementation in [Demo.scala](Demo_ScalaEnv/src/main/scala/Demo.scala) and [Demo2.scala](Demo_ScalaEnv/src/main/scala/Demo2.scala)
```
./run_spark.sh

# in spark shell
scala> :load projects/Demo_ScalaEnv/src/main/scala/Demo.scala
Loading projects/Demo_ScalaEnv/src/main/scala/Demo.scala...
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
defined object Demo

scala> Demo.main(Array("", "20240620_1010", "test.out"))
args=, 20240620_1010, test.out
BEGIN: 2024-06-24 09:16:26
DONE: 2024-06-24 09:16:26
```

```
scala> :load /opt/spark/work-dir/projects/Demo_ScalaEnv/src/main/scala/Demo2.scala
Loading /opt/spark/work-dir/projects/Demo_ScalaEnv/src/main/scala/Demo2.scala...
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import org.apache.spark.sql.SparkSession
defined object Demo

scala> Demo.main(Array("demo2", "20240601_0000", "test.out"))
args=demo2, 20240601_0000, test.out
BEGIN: 2024-06-24 12:09:53
A_info.obj_x.value, float
DONE: 2024-06-24 12:09:53
```

### Build and Submit tasks
```
./run_container.sh
# in container
cd projects/Demo_ScalaEnv/
sbt compile
sbt package
ls target/scala-2.12/demo_2.12-0.0.jar
# exit container
```

```
# execute defined task
projects/Demo_ScalaEnv/tasks/run_demo_scala.sh
```
