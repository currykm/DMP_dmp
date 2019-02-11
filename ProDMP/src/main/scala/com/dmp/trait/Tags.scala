package com.dmp.`trait`

trait Tags {
        //  打标签的方法
        //Any 类型不确定 * 数量不确定
        //返回值：String，double(权重)
        def makeTags(args:Any*):Map[String,Double]
}
