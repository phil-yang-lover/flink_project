package com.yang.gmall.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/3
 * @Time: TODO 22:10
 * @Description: TODO :
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}
