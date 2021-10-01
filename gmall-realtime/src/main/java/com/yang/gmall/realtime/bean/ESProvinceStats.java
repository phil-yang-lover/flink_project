package com.yang.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/2
 * @Time: TODO 11:25
 * @Description: TODO :
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ESProvinceStats {

    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
    private Double order_amount;
    private Long  order_count;
    private Long ts;

    public ESProvinceStats(OrderWide orderWide){
        province_id = orderWide.getProvince_id();
        order_amount = Double.valueOf(orderWide.getSplit_total_amount().floatValue());
        province_name=orderWide.getProvince_name();
        area_code=orderWide.getProvince_area_code();
        iso_3166_2=orderWide.getProvince_iso_code();
        iso_code=orderWide.getProvince_iso_code();

        order_count = 1L;
        ts=new Date().getTime();
    }
    public ESProvinceStats(ProvinceStats provinceStats){
        province_id = provinceStats.getProvince_id();
        order_amount = provinceStats.getOrder_amount().doubleValue();
        province_name=provinceStats.getProvince_name();
        area_code=provinceStats.getArea_code();
        iso_3166_2=provinceStats.getIso_3166_2();
        iso_code=provinceStats.getIso_code();
        order_count = provinceStats.getOrder_count();
        ts=provinceStats.getTs();
    }
}
