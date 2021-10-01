package com.yang.gmall.controller;

import com.yang.gmall.beans.ProductStats;
import com.yang.gmall.beans.ProvinceStats;
import com.yang.gmall.beans.VisitorStats;
import com.yang.gmall.service.ProductStatsService;
import com.yang.gmall.service.ProvinceStatsService;
import com.yang.gmall.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/4/30
 * @Time: TODO 22:42
 * @Description: TODO :
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProvinceStatsService provinceStatsService;

    //TODO : 商品主题
    @Autowired
    ProductStatsService productStatsService;
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date" ,defaultValue = "0") Integer date){
        if (date == 0) date=now();
        BigDecimal gmv = productStatsService.getGMV(date);
        return "{\"status\": 0,\"data\": " + gmv + "}";
    }

    //TODO : 省市主题
    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date" ,defaultValue = "0") Integer date){

        if(date ==0 ) date=now();

        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        StringBuilder builder = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceStatsList.size(); i++) {
            ProvinceStats provinceStats = provinceStatsList.get(i);
            builder.append("{\"name\": \""+provinceStats.getProvince_name()+"\",\"value\": "+provinceStats.getOrder_amount()+"}");
            if(i < provinceStatsList.size() - 1){
                builder.append(",");
            }
        }

        builder.append("],\"valueName\": \"交易额\"}}");
        return builder.toString();
    }
    @Autowired
    VisitorStatsService visitorStatsService;
    @RequestMapping("/hello")
    public String hello(){
        return "hello";
    }

    //TODO : 访客主题
    @RequestMapping("/visitor")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date){
        return null;
    }

    private Integer now() {
        return Integer.parseInt(DateFormatUtils.format(new Date(),"yyyyMMdd"));
    }
}
