package org.apache.spark.yao

class StatusTableSuite extends BaseSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    StatusStoreCatalog.registerUDTs()
  }

  test("tableExists") {
    assert(StatusTable.tableExists("application"))
    assert(!StatusTable.tableExists("non_existent_table"))
  }

  test("listTables") {
    val tables = StatusTable.listTables()
    assert(tables.contains("application"))
    assert(tables.length > 0)
  }
}
