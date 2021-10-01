package com.yang.gmall.service;

import com.yang.gmall.beans.ProvinceStats;
import com.yang.gmall.mapper.ProvinceStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/2
 * @Time: TODO 18:04
 * @Description: TODO :
 */
public interface ProvinceStatsService {
   List<ProvinceStats> getProvinceStats(Integer date);
}
