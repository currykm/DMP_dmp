package com.dmp.tools.ips

import java.util

import com.dmp.bean.Longitude_Latitude_region_city
import com.dmp.tools.GlobalConfigUtils
import com.dmp.tools.ips.iplocation.{IPAddressUtils, IPLocation}
import com.maxmind.geoip.{Location, LookupService}

import scala.collection.JavaConverters


//todo:解析经纬度和城市，并进行封装
object ParseIP2La_long {
  //根据GeoLiteCity.dat解析经纬度
  //根据qqwry.dat解析城市
  val GeoLitecity: String = GlobalConfigUtils.getGeoLiteCity

  //定义一个方法：ipList: List[String] ---> Seq(La_lo(ip , la , lo , region , city))
  def parse2bean(ipList: List[String]): Seq[Longitude_Latitude_region_city] = {
    //根据ip解析出经纬度
    val look = new LookupService(GeoLitecity, LookupService.GEOIP_MEMORY_CACHE)
    //定义一个List集合
    val array = new util.ArrayList[Longitude_Latitude_region_city]()
    for (ip <- ipList) {
      val location: Location = look.getLocation(ip)
      //纬度
      val latitude: Float = location.latitude
      //经度
      val longitude: Float = location.longitude
      //使用纯真数据库qqwry.dat解析出IP所在的省份和城市
      val iPAddressUtils = new IPAddressUtils()
      val locations: IPLocation = iPAddressUtils.getregion(ip)
      val region: String = locations.getRegion
      //省
      val city: String = locations.getCity //市
      array.add(Longitude_Latitude_region_city(ip, latitude + "", longitude + "", region, city))


    }
    //contervers 转换器
    val toSeq: Seq[Longitude_Latitude_region_city] = JavaConverters.asScalaIteratorConverter(array.iterator()).asScala.toSeq
    toSeq
  }

}
