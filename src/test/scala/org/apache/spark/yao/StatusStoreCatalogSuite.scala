package org.apache.spark.yao

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.yao.encoder.{APPLICATION, EXECUTOR, JOB}

class StatusStoreCatalogSuite extends BaseSuite {

  private var catalog: StatusStoreCatalog = _
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("StatusStoreCatalogSuite")
      .config("spark.sql.catalog.live", "org.apache.spark.yao.StatusStoreCatalog")
      .getOrCreate()
    catalog = spark.sessionState.catalogManager.catalog("live").asInstanceOf[StatusStoreCatalog]
    super.beforeAll()
  }

  test("initialize catalog") {
    assert(catalog.name() == "live")
  }

  test("listTables") {
    val tables = catalog.listTables(Array.empty)
    assert(tables.nonEmpty)
    assert(tables.exists(_.name() === "application"))
  }

  test("tableExists") {
    assert(catalog.tableExists(Identifier.of(Array.empty, "application")))
    assert(!catalog.tableExists(Identifier.of(Array.empty, "non_existent_table")))
  }

  test("loadTable") {
    val table = catalog.loadTable(Identifier.of(Array.empty, "application"))
    assert(table != null)
    assert(table.schema().fields.nonEmpty)
  }

  test("scan application table") {
    val df = spark.table("live.application")
    assert(df.count() > 0)
    assert(df.schema === APPLICATION.schema)
    assert(df.collect().head.get(0) != null)
    assert(df.collect().head.get(1) === "StatusStoreCatalogSuite")
  }

  test("scan executor table") {
    val df = spark.table("live.executor")
    assert(df.count() > 0)
    assert(df.schema === EXECUTOR.schema)
    assert(df.collect().head.get(0)  === "driver")
    assert(df.collect().head.getBoolean(2))
  }

  test("scan job table") {
    spark.sparkContext.setJobDescription("scan job table")
    val df = spark.table("live.job").cache().where("jobId = 0")
    assert(df.count() === 1)
    assert(df.schema === JOB.schema)
  }

  test("scan non-existent table") {
    val e = intercept[Exception] {
      spark.table("live.non_existent_table")
    }
    assert(e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"))

    println("Expected exception: ", e)
  }

  test("scan rdd table") {
    val df = spark.table("live.rdd")
    assert(df.count()  === 1)
    assert(df.head().getAs[String]("name").startsWith("LocalTableScan"))
  }

}
