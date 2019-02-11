package com.dmp

import com.dmp.ETL._
import com.dmp.tools.GlobalConfigUtils
import com.dmp.trading.TradingArea
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//todo:spark执行不同逻辑的代码
object APP {
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  def main(args: Array[String]): Unit = {
    //transient 防止序列化
    @transient
    val sparkConf = new SparkConf().setAppName("APP")
      //设置本地3个线程
      .setMaster("local[3]")
      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuuferSize)
      .set("cluster.name" , GlobalConfigUtils.clusterName)
      .set("es.index.auto.create" , GlobalConfigUtils.autoCreateIndex)
      .set("es.nodes" , GlobalConfigUtils.esNodes)
      .set("es.port" , GlobalConfigUtils.esPort)
      .set("es.index.reads.missing.as.empty" , GlobalConfigUtils.isMissing)
      .set("es.nodes.discovery" , GlobalConfigUtils.esNodesDiscovery)
      .set("es.nodes.wan.only" , GlobalConfigUtils.wanOnly)
      .set("es.http.timeout" , GlobalConfigUtils.esTimeout)
    //创建sparkContext
      val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)
    //创建sqlContext
      val sqlContext: SQLContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    //创建kuduContext
    val kuduContext = new KuduContext(KUDU_MASTER,sqlContext.sparkContext)
    //todo:1://根据IP解析出经纬度和ip所在的城市
    //ImproveData.process(sqlContext,sparkContext,kuduContext)
    //todo:2地域分布情况
    //ProcessRegion_city.process(sqlContext,sparkContext,kuduContext)
    //todo:3:广告地域分布情况
    //RegionalAnalysis.process(sqlContext,sparkContext,kuduContext)
    //todo:4:APP地域分布情况
    APPAnalysis.process(sqlContext,sparkContext,kuduContext)
    //todo:5:手机终端分布情况
    DeviceType.process(sqlContext,sparkContext,kuduContext)
    //todo:6).网络运行商分布情况
    //todo:生成商圈库
    TradingArea.process(sqlContext,sparkContext,kuduContext)





    //关闭SparkContext
    if(!sparkContext.isStopped){
      sparkContext.stop()
    }

  }

}
