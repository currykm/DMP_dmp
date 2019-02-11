package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object NetworkOpertorAnalysis extends ProcessData{
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = ???
}
