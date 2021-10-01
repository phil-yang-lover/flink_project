package com.yang.gmall.service.Impl;

import com.yang.gmall.beans.ProductStats;
import com.yang.gmall.mapper.ProductStatsMapper;
import com.yang.gmall.service.ProductStatsService;
import com.yang.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/3
 * @Time: TODO 19:38
 * @Description: TODO :
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    //TODO : 不想注入的对象属性显示红线，就在mapper接口上面加@Repository或者修改它的警告级别
    ProductStatsMapper productStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return productStatsMapper.selectGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTrademark(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategroy3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategroy3(date, limit);
    }
}
