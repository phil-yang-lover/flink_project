package com.yang.gmall.mapper;

import com.yang.gmall.beans.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/4/30
 * @Time: TODO 16:27
 * @Description: TODO :
 */
public interface VisitorStatsMapper {
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct,sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
        " from visitor_stats_2021 where toYYYYMMDD(stt)=#{date} group by is_new;")
    public List<VisitorStats> selectVisitorStatsByNewFlag(Integer date);
}
