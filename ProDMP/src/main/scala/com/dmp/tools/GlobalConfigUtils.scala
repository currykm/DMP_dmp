package com.dmp.tools

import com.typesafe.config.{Config, ConfigFactory}

class GlobalConfigUtils {
  private val conf: Config = ConfigFactory.load()
  //开始加载spark相关的配置参数
  def sparkWorkTimeout = conf.getString("spark.worker.timeout")
  def sparkRpcTimeout = conf.getString("spark.rpc.askTimeout")
  def sparkNetWorkTimeout = conf.getString("spark.network.timeoout")
  def sparkMaxCores = conf.getString("spark.cores.max")
  def sparkTaskMaxFailures = conf.getString("spark.task.maxFailures")
  def sparkSpeculation = conf.getString("spark.speculation")
  def sparkAllowMutilpleContext = conf.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer = conf.getString("spark.serializer")
  def sparkBuuferSize = conf.getString("spark.buffer.pageSize")

  //加载ES相关的配置文件
  def clusterName = conf.getString("cluster.name")
  def autoCreateIndex = conf.getString("es.index.auto.create")
  def esNodes = conf.getString("esNodes")
  def esPort = conf.getString("es.port")
  def isMissing = conf.getString("es.index.reads.missing.as.empty")
  def esNodesDiscovery = conf.getString("es.nodes.discovery")
  def wanOnly = conf.getString("es.nodes.wan.only")
  def esTimeout = conf.getString("es.http.timeout")
  //获取kudu的相关配置参数
  def kuduMaster = conf.getString("kudu.master")
  //获取数据源路径
  def getDataPath=conf.getString("data.path")
  //获取ip经纬度的参数
  def  getGeoLiteCity = conf.getString("GeoLiteCity.dat")
  //加载纯真数据库
  def IP_FILE = conf.getString("IP_FILE")
  def INSTALL_DIR=conf.getString("INSTALL_DIR")
  //加载ods的前缀
  def ODS_PREFIX=conf.getString("DB.ODS.PREFIX")
  //加载地域分布数据
  def  ProcessRegionCity=conf.getString("ProcessRegionCity")
  //加载广告地域分布情况
  def AdvertisingAnalytis = conf.getString("AdvertisingAnalytis")
  //加载广告APP分布情况
  def APPAnalysis = conf.getString("APPAnalysis")
  //加载广告终端分布情况
  def DeviceType = conf.getString("DeviceType")
  //加载高德key
  def key = conf.getString("key")
  //加载商圈落地表
  def trade =  conf.getString("Trade")
  //加载appname
  def appID_name=conf.getString("AppIDName")
  //加载devicedic
  def devicedic=conf.getString("devicedic")



}
object GlobalConfigUtils extends GlobalConfigUtils
