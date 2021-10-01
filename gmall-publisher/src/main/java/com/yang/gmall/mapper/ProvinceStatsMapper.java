package com.yang.gmall.mapper;

import com.yang.gmall.beans.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/2
 * @Time: TODO 17:57
 * @Description: TODO :
 */
public interface ProvinceStatsMapper {
    @Select("select province_id,province_name,sum(order_amount) order_amount " +
        " from province_stats_2021 where toYYYYMMDD(stt)=#{date} " +
        " group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(Integer date);
}
