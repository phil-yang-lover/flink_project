package com.yang.gmall.service.Impl;

import com.yang.gmall.beans.ProvinceStats;
import com.yang.gmall.mapper.ProvinceStatsMapper;
import com.yang.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/2
 * @Time: TODO 18:06
 * @Description: TODO :
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(Integer date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
