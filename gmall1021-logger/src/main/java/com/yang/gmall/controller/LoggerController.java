package com.yang.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String logJsonString ){
        //TODO : 打印在控制台
        //System.out.println(logJsonString);
        //TODo: 落盘
        log.info(logJsonString);
        //TODo: 接入kafka
        kafkaTemplate.send("ods_base_log",logJsonString);
        return "success";
    }
}
