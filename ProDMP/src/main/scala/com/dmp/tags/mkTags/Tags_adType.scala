package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.spark.sql.Row

object Tags_adType extends Tags{
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String,Double]()
    //首先判断传过来的参数
    if(args.length>0){
      //scala中的强制类型转换
      val row: Row = args(0).asInstanceOf[Row]
      //adspacetype广告位类型
      val adspacetype: Int = row.getAs[Long]("adspacetype").toInt
      if(adspacetype!=null || adspacetype!=""){
        //广告位类型：1.banner；2.插屏 3.全屏
        adspacetype match {
          case x if x ==1 =>map+=("LC"+x ->1)
          case x if x ==2 =>map+=("LC"+x ->1)
          case x if x ==3 =>map+=("LC"+x ->1)

        }
      }

    }
    map
  }
}
