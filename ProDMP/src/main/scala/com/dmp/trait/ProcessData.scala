package com.dmp.`trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
//todo:特指：抽取公共的方法，
trait ProcessData {
  def process(sQLContext: SQLContext,sparkContext: SparkContext,kuduContext: KuduContext)

}
