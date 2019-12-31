package lu.flink.table.tableapi

import java.util.Properties

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaTableSource}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}

object CreateTableEnvironmentExample {
  def main(args: Array[String]): Unit = {
    // **********************
    // FLINK STREAMING QUERY
    // **********************
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)

    val csvTableSource: CsvTableSource = CsvTableSource
      .builder()
      .path("/home/lu/test.csv")
      .fieldDelimiter(",")
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .build()
    fsTableEnv.registerTableSource("kafka", csvTableSource)

    // ******************
    // FLINK BATCH QUERY
    // ******************
    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val fbTableEnv = BatchTableEnvironment.create(fbEnv)

    // register a table
    val table: Table = fsTableEnv.scan("X").select()
    fbTableEnv.registerTable("test", table)


    // **********************
    // BLINK STREAMING QUERY
    // **********************
    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    // or val bsTableEnv = TableEnvironment.create(bsSettings)

    // ******************
    // BLINK BATCH QUERY
    // ******************
    val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    //val bbTableEnv = TableEnvironment.create(bbSettings)
  }
}
