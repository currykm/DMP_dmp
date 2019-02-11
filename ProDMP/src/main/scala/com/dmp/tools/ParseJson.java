package com.dmp.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dmp.bean.BusinessAreas;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class ParseJson {
    public static String parse(String json){
        //json 解析object ----> object 是所有类的父类
        JSONObject jsonObject = JSON.parseObject(json);
        /**<regeocode>
        //<addressComponent>
        //<businessAreas type="list">
         <businessArea>
         </businessArea>
        // </businessAreas>
        //</addressComponent>
        // </regeocode>
         **/
        //解析regeocode
        JSONObject  regeocode =(JSONObject) jsonObject.get("regeocode");
        JSONObject  addressComponent =(JSONObject) regeocode.get("addressComponent");
        //获取list集合
        JSONArray businessAreas = addressComponent.getJSONArray("businessAreas");
        //解析JSON数组，返回BusinessAreas的集合
        List<BusinessAreas> list = JSON.parseArray(businessAreas.toJSONString(), BusinessAreas.class);

        StringBuffer sb  = new StringBuffer();
        for(int i=0;i<list.size()-1;i++){
            if(list.get(i).getName()!=null && StringUtils.isNotBlank(list.get(i).getName())){
                sb.append(list.get(i).getName()).append(":");
            }
        }
        if(StringUtils.isNotBlank(sb.toString())){
            String data  = sb.toString();
            return data.substring(0,data.length()-1);
        }else{
            return "blank";
        }




    }
}
