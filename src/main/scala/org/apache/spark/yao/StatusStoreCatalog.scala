package org.apache.spark.yao

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructType, UDTRegistration}
import org.apache.spark.yao.StatusStoreCatalog.registerUDTs
import org.apache.spark.yao.encoder.udt.{ExecutorMetricsUDT, JavaUtilDateUDT, ResourceInformationUDT}

// TODO: validate namespace
class StatusStoreCatalog extends TableCatalog {
  private var catalog: String = _
  private var spark: SparkSession = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalog = name
    // Initialize the catalog with the provided options if necessary
    spark = SparkSession.getActiveSession
      .getOrElse(throw new IllegalStateException("No active Spark session found."))
    registerUDTs()
  }

  override def name(): String = catalog

  override def tableExists(ident: Identifier): Boolean = {
    StatusTable.tableExists(ident.name())
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    StatusTable.listTables().map { tableName =>
      Identifier.of(namespace, tableName)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    StatusTable.tableExists(ident.name(), throwable = true)
    StatusTable(spark, ident.name())
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("createTable is not supported in StatusStoreCatalog")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable is not supported in StatusStoreCatalog")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("dropTable is not supported in StatusStoreCatalog")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable is not supported in StatusStoreCatalog")
  }
}

object StatusStoreCatalog {
  def registerUDTs(): Unit = {
    Seq(
      JavaUtilDateUDT,
      ExecutorMetricsUDT,
      ResourceInformationUDT
    ).foreach { udt =>
      UDTRegistration.register(udt.userClass.getName, udt.getClass.getName.stripSuffix("$"))
    }
  }
}
