package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object DeviceType extends ProcessData{
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE=GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()
  val SINK_TABLE=GlobalConfigUtils.DeviceType
  val KuduOptions:Map[String,String]=Map(
    "kudu.master"->KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //1.1读取数据
    val ods: DataFrame = sQLContext.read.options(KuduOptions).kudu
    ods.registerTempTable("ods")
    val dataFrame: DataFrame = sQLContext.sql(ConstantsSQL.DeviceType)
    dataFrame.registerTempTable("temp_table")
    val result: DataFrame = sQLContext.sql(ConstantsSQL.deviceAnalysis)
    val schema: Schema = ConstantsSchema.deviceAnalysis
    val partitionID="client"
    DBUtils.process(kuduContext,result,SINK_TABLE,KUDU_MASTER,schema,partitionID)

  }
}
