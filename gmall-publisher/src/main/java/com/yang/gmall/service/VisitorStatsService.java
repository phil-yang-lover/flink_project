package com.yang.gmall.service;

import com.yang.gmall.beans.VisitorStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/4/30
 * @Time: TODO 16:57
 * @Description: TODO :
 */
public interface VisitorStatsService {
    public List<VisitorStats> getVisitorStatsByNewFlag(Integer date);
}
