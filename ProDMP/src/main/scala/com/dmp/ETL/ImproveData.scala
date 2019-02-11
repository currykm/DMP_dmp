package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.bean.Longitude_Latitude_region_city
import com.dmp.tools.ips.ParseIP2La_long
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//todo:加载数据：实现加载数据的接口
object ImproveData extends ProcessData{
  val TO_TABLENAME=GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()
val DATA_PATH = GlobalConfigUtils.getDataPath
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster


  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
  val jsondata: DataFrame = sQLContext.read.format("json").load(DATA_PATH)
    //todo1:根据IP解析出经纬度和省
    //DataFrame转化为rdd
    val rdd: RDD[Row] = jsondata.select("ip").rdd

    val ipstr: RDD[String] = rdd.map {
          //函数的转变
      line => line.getAs[String]("ip")
    }
    //把所有的IP拿出来，封装到一个List集合里面
    val ipLIst: List[String] = ipstr.collect().toBuffer.toList
    //ipList: List[String] ---> Seq(La_lo(ip , la , lo , region , city))
    val parse2bean: Seq[Longitude_Latitude_region_city] = ParseIP2La_long.parse2bean(ipLIst)
    //--->paralize(Seq())-->rdd
    val rddBean: RDD[Longitude_Latitude_region_city] = sparkContext.parallelize(parse2bean)
    //--->DF--->kudu
    import sQLContext.implicits._
    //RDD转化为DF
    val dF: DataFrame = rddBean.toDF
    //创建表
    jsondata.registerTempTable("ods")
    //创建解析经纬度和省市
    dF.registerTempTable("La_lo_region_city")
    //执行sql
    val result: DataFrame = sQLContext.sql(ConstantsSQL.odssql)
    //result.show(10)
    //需要落地到kudu
    /**
      * kuduContext:KuduContext,
      * data:DataFrame,
        TO_TABLENAME:String,
         KUDU_MASTER:String,
         schema: Schema,
         partitionID:String
      * */
    val schema: Schema = ConstantsSchema.odsSchema
    val partitionID="ip"
    DBUtils.process(kuduContext,result,TO_TABLENAME,KUDU_MASTER,schema,partitionID)










  }
}
