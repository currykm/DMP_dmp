package com.dmp.tools

import java.util

import org.apache.kudu.{Schema, client}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{DataFrame, SaveMode}
//todo:数据落地
object DBUtils {
  def process(
               kuduContext:KuduContext,
               data:DataFrame,
               TO_TABLENAME:String,
               KUDU_MASTER:String,
               schema: Schema,
               partitionID:String
             ): Unit ={
//todo:如果不存在，则创建表
    if(!kuduContext.tableExists(TO_TABLENAME)){
      //创建kudu的客户端，创建表
      val kuduClient: KuduClient = new KuduClientBuilder(KUDU_MASTER).build()
      //定义表的分区分式
      val createTableOptions:CreateTableOptions = {
        //需要一个链表，用于携带kudu的分区ID
        val parcols = new util.LinkedList[String]()
        parcols.add(partitionID)
        //指定kudu的分区方式
        new client.CreateTableOptions()
          .addHashPartitions(parcols,6)//哈希分区
          //设置副本
          .setNumReplicas(3)
      }
      //调用api，创建表
      kuduClient.createTable(TO_TABLENAME,schema,createTableOptions)
    }

    //todo:将数据写入kudu  追加覆盖
    data.write
      .mode(SaveMode.Append)
      .option("kudu.table",TO_TABLENAME)
      .option("kudu.master",KUDU_MASTER)
      .kudu
  }

}
