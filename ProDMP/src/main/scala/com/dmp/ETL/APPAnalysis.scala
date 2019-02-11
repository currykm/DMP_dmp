package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

object APPAnalysis extends ProcessData{
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()
  val SINK_TABLE=GlobalConfigUtils.APPAnalysis
  val kuduOptions:Map[String,String]=Map(
    "kudu.master"->KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )



  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //1.查看APP分布情况,
    //1.1获取数据
 val ods: DataFrame = sQLContext.read.options(kuduOptions).kudu
    ods.registerTempTable("ods")
    val dataFrame: DataFrame = sQLContext.sql(ConstantsSQL.appAnalysis_tmp)
    dataFrame.registerTempTable("temp_table")
    val result: DataFrame = sQLContext.sql(ConstantsSQL.adRegionAnalysis)
//schema
    val aPPAnalysis: Schema = ConstantsSchema.APPAnalysis
    val partitionID="appid"
    //数据落地
    DBUtils.process(kuduContext,result,SINK_TABLE,KUDU_MASTER,aPPAnalysis,partitionID)






    //1.2报表
    //1.3数据落地


  }
}
