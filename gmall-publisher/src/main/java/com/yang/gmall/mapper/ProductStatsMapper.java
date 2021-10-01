package com.yang.gmall.mapper;

import com.yang.gmall.beans.ProductStats;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/3
 * @Time: TODO 19:34
 * @Description: TODO :
 */
@Repository
public interface ProductStatsMapper {
    @Select("select sum(order_amount) " +
        " from product_stats_2021 where toYYYYMMDD(stt)=#{date} ")
    public BigDecimal selectGMV(Integer date);


    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
        " from product_stats_2021 where toYYYYMMDD(stt)=#{date} " +
        " group by tm_id,tm_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByTrademark(Integer date,Integer limit);

    @Select("select category3_id,category3_name,sum(order_amount) order_amount " +
        " from product_stats_2021 " +
        " where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name " +
        " having order_amount>0 order by order_amount desc limit #{limit} ")
    List<ProductStats> selectProductStatsByCategroy3(Integer date,Integer limit);
}
