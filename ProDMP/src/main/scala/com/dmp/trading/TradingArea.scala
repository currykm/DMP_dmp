package com.dmp.trading

import ch.hsr.geohash.GeoHash
import com.dmp.`trait`.ProcessData
import com.dmp.bean.Barea
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD

object TradingArea extends ProcessData{
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE=GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()
  val SINK_TABLE=GlobalConfigUtils.trade
  val kuduOptions:Map[String,String] = Map(
    "kudu_master"->KUDU_MASTER,
    "kudu_tables"->SOURCE_TABLE

  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //获取数据源
    val data: DataFrame = sQLContext.read.options(kuduOptions).kudu
    //过滤非中国的经纬度(73<long<136,3<lat<54)
    //创建临时表
    data.registerTempTable("ods")
    val long_lat: DataFrame = sQLContext.sql(ConstantsSQL.filterNotChina)
    //dataframe 转化为rdd
    val rdd: RDD[Row] = long_lat.rdd
    //rdd的算子操作
    val trade =rdd.map{
      line =>
        val long = line.getAs[String]("long")
        val lat = line.getAs[String]("lat")
        //Precision 精确度
        val geoHashCode: String = GeoHash.withCharacterPrecision(long.toDouble,lat.toDouble,8).toBase32
        val location = long+","+lat
        val trade: String = ExtractTrading.getArea(location)
        Barea(location,trade)
    }.filter{line => !line.trade.equals("blank")}

    //数据落地，生成自己的商圈库
    //隐式转换(rdd转换为DataFrame)
    import sQLContext.implicits._
    val result: DataFrame = trade.toDF("geohashcode","trade")
    val schema: Schema = ConstantsSchema.trade
    val partitionID="geohashcode"
    DBUtils.process(kuduContext,result,SINK_TABLE,KUDU_MASTER,schema,partitionID)





  }
}
