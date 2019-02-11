package com.dmp.tools

object ConstantsSQL {
  //1：初始化，将经纬度和地域merge到ods中
  //lazy 表示不被初始化,延迟加载
  lazy val odssql = "select " +
    "ods.ip ," +
    "ods.sessionid," +
    "ods.advertisersid," +
    "ods.adorderid," +
    "ods.adcreativeid," +
    "ods.adplatformproviderid" +
    ",ods.sdkversion" +
    ",ods.adplatformkey" +
    ",ods.putinmodeltype" +
    ",ods.requestmode" +
    ",ods.adprice" +
    ",ods.adppprice" +
    ",ods.requestdate" +
    ",ods.appid" +
    ",ods.appname" +
    ",ods.uuid, ods.device, ods.client, ods.osversion, ods.density, ods.pw, ods.ph" +
    ",La_lo_region_city.longitude as long" +
    ",La_lo_region_city.latitude as lat" +
    ",La_lo_region_city.region as provincename" +
    ",La_lo_region_city.city as cityname" +
    ",ods.ispid, ods.ispname" +
    ",ods.networkmannerid, ods.networkmannername, ods.iseffective, ods.isbilling" +
    ",ods.adspacetype, ods.adspacetypename, ods.devicetype, ods.processnode, ods.apptype" +
    ",ods.district, ods.paymode, ods.isbid, ods.bidprice, ods.winprice, ods.iswin, ods.cur" +
    ",ods.rate, ods.cnywinprice, ods.imei, ods.mac, ods.idfa, ods.openudid,ods.androidid" +
    ",ods.rtbprovince,ods.rtbcity,ods.rtbdistrict,ods.rtbstreet,ods.storeurl,ods.realip" +
    ",ods.isqualityapp,ods.bidfloor,ods.aw,ods.ah,ods.imeimd5,ods.macmd5,ods.idfamd5" +
    ",ods.openudidmd5,ods.androididmd5,ods.imeisha1,ods.macsha1,ods.idfasha1,ods.openudidsha1" +
    ",ods.androididsha1,ods.uuidunknow,ods.userid,ods.iptype,ods.initbidprice,ods.adpayment" +
    ",ods.agentrate,ods.lomarkrate,ods.adxrate,ods.title,ods.keywords,ods.tagid,ods.callbackdate" +
    ",ods.channelid,ods.mediatype,ods.email,ods.tel,ods.sex,ods.age from ods left join " +
    "La_lo_region_city on ods.ip=La_lo_region_city.ip where ods.ip is not null"
  //地域数量sql
  lazy val region_city_sql = "select  provincename,cityname,count(1) as NUM from ods group by provincename,cityname "
  //广告投放分布
  lazy val advertisingRegion="select provincename,cityname ," +
    "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) OriginalRequest," +
    "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) EffectiveRequest," +
    "sum(case when requestmode=1 and processnode =3  then 1 else 0 end) AdvertisingRequest,sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 and adplatformproviderid >=100000 then 1 else 0 end) bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus, sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adImpressions, " +
    "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) adClicks, sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) MediumDisplayNum, sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) MediumClickNum, " +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >= 100000 and adorderid > 200000 and adcreativeid > 200000 then 1*adpayment/1000 else 0 end) adCost,sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >= 100000 and adorderid > 200000 and adcreativeid > 200000 then 1*winprice/1000 else 0 end) adConsumption " +
    "from ods  group by provincename, cityname"
  //获取有用的字段名
  lazy val adRegionAnalysis = "select " +
    "provincename , " +
    "cityname , " +
    "OriginalRequest , " +
    "ValidRequest , " +
    "adRequest , " +
    "bidsNum , " +
    "bidSus , " +
    "bidSus/bidsNum bidsSusRat , " +
    "adImpressions , " +
    "adClicks , " +
    "adClicks/adImpressions adClickRat ," +
    "adCost , " +
    "adConsumption " +
    "from regionTemp where bidsNum != 0 and adImpressions != 0"
  //APP分布情况
  //4：广告投放的app分布情况统计
  lazy val appAnalysis_tmp = "select " +
    "appid," +
    "appname , " +
    "sum(case when requestmode<=2 and processnode=1 then 1 else 0 end) OriginalRequest ," +
    "sum(case when requestmode>=1 and processnode >= 2 then 1 else 0 end) ValidRequest , " +
    "sum(case when requestmode=1 and processnode = 3 then 1 else 0 end) adRequest , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid != 0 then 1 else 0 end) bidsNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end) bidsSus ," +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adImpressions , " +
    "sum(case when requestmode=3 and iseffective=1 and adorderid != 0 then 1 else 0 end) adClicks , " +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) MediumDisplayNum , " +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) MediumClickNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then 1*winprice/1000 else 0 end) adCost , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbid=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then 1*adpayment/1000 else 0 end) adConsumption " +
    "from ods group by appid,appname"


  lazy val appAnalysis = "select " +
    "appid , " +
    "appname , " +
    "OriginalRequest , " +
    "ValidRequest , " +
    "adRequest , " +
    "bidsNum , " +
    "bidsSus , " +
    "bidsSus/bidsNum bidsSusRat , " +
    "adImpressions , " +
    "adClicks , " +
    "adClicks/adImpressions adClickRat ," +
    "adCost , " +
    "adConsumption " +
    "from temp_table where bidsNum != 0 and adImpressions != 0"

  ////TODO手机设备
  lazy val DeviceType="select " +
    "case client when 1 then 'ios'" +
    "when 2 then 'android'" +
    "when 3 then 'wp'" +
    "else 'OTHER' end client," +
    "device," +
    "sum(case when requestmode<=2 and processnode =1 then 1 else 0 end)OriginalRequest,sum(case when requestmode>=1 and processnode >=2 then 1 else 0 end)EffectiveRequest," +
    "sum(case when requestmode=1 and processnode 3 then 1 else 0 end)ADrequestnumber,sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end)bidnumber," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end)Bidsuccessnumber,sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)shownumber," +
    "sum(case when requestmode=3 and iseffective=1 and adorderid!=0 then 1 else 0 end)clicknumber,sum(case when requestmode=2 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end)mediumShowNumber," +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end)mediumClickNumber,sum(case when adplatformproviderid>=100000 and iseffective=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then 1*winprice/1000 else 0 end)ADConsume," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and iswin=1 and isbid=1 and  adorderid>200000 and adcreativeid>200000 then 1*adpayment/1000 else 0 end)ADCost " +
    "from ods group by client,device"


  lazy val deviceAnalysis="select " +
    "client," +
    "device," +
    "OriginalRequest," +
    "EffectiveRequest," +
    "ADrequestnumber," +
    "bidnumber," +
    "Bidsuccessnumber," +
    "Bidsuccessnumber/bidnumber BidsuccessRat," +
    "shownumber," +
    "clicknumber," +
    "clicknumber/shownumber clicknumberRat," +
    "ADConsume," +
    "ADCost" +
    "from temp_table bidnumber!=0 and shownumber!=0"

  lazy val filterNotChina="select distinct long,lat from ods where long>73 and long<136 and lat >3 and lat <56"
  lazy val non_empty_UID=
    """
      |imei !="" or imeimd5 !="" or imeisha1 !="" or
      |mac != "" or macmd5 != "" or macsha1 != "" or
      |idfa != "" or idfamd5 != "" or idfasha1 != "" or
      |openudid != "" or openudidmd5 != "" or openudidsha1 != "" or
      |androidid != "" or androididmd5 != "" or androididsha1 != ""
    """.stripMargin


}
