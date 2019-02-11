package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
//todo:地域数量分布
object ProcessRegion_city extends ProcessData{
  //获取kudu的相关配置参数
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  //数据源
  val SOURCE_TABLE=GlobalConfigUtils.ODS_PREFIX+"20190116"
    //DateUtils.NowDate()
  //目的地
   val SINK_TABLE=GlobalConfigUtils.ProcessRegionCity
  //
  val kuduOption:Map[String,String]=Map(
    "kudu.table" ->SOURCE_TABLE,
    "kudu.master"->KUDU_MASTER
  )



  /**
    * 去查询【ODS+当天】表，统计低于数量分布情况
    * */
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit ={
    //1.读取kudu中的数据sourceTable
    val ods: DataFrame = sQLContext.read.options(kuduOption).kudu
    ods.registerTempTable("ods")
    //2.执行SQL。查询地域数量分布情况
    val result: DataFrame = sQLContext.sql(ConstantsSQL.region_city_sql)
    //3.数据落地
    //3.1构建表的schema
    val processRegioncity: Schema = ConstantsSchema.processRegioncity
    //3.2构建分区ID
    val partitionID = "provincename"
    //3.3数据落地
    /**
      * kuduContext:KuduContext,
          data:DataFrame,
         TO_TABLENAME:String,
          KUDU_MASTER:String,
          schema: Schema,
            partitionID:String
      * */

    DBUtils.process(kuduContext,result,SINK_TABLE,KUDU_MASTER,processRegioncity,partitionID)
  }
}
