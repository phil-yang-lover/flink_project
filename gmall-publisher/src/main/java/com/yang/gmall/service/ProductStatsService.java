package com.yang.gmall.service;

import com.yang.gmall.beans.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/3
 * @Time: TODO 19:37
 * @Description: TODO :
 */
public interface ProductStatsService {
    public BigDecimal getGMV(Integer date);

    List<ProductStats> getProductStatsByTrademark(Integer date,Integer limit);

    List<ProductStats> getProductStatsByCategroy3(Integer date,Integer limit);
}
