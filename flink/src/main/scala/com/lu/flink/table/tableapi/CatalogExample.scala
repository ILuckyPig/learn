package lu.flink.table.tableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.ExternalCatalog

object CatalogExample {
  def main(args: Array[String]): Unit = {
    // The Blink planner does not support external catalog.
    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val fbTableEnv = BatchTableEnvironment.create(fbEnv)

    // create an external catalog
    //val catalog: ExternalCatalog = new InMemoryExternalCatalog

    // register the ExternalCatalog catalog
    //fbTableEnv.registerExternalCatalog("InMemCatalog", catalog)
  }
}
