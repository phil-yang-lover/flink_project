package com.yang.gmall.service.Impl;

import com.yang.gmall.beans.VisitorStats;
import com.yang.gmall.mapper.VisitorStatsMapper;
import com.yang.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/4/30
 * @Time: TODO 16:59
 * @Description: TODO :
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(Integer date) {
        System.out.println("============");
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }
}
