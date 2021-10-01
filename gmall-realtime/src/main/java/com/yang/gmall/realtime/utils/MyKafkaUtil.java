package com.yang.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String KAFKA_SERVER = "hadoop2101:9092,hadoop2102:9092,hadoop2103:9092";
    private static String DEFAULT_TOPIC="default_topic";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic , String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);
    }
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,60*1000*15+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic,String groupId){
        String ddl = "'connector' = 'kafka'," +
            "'topic' = '"+topic+"'," +
            "'properties.bootstrap.servers' = '"+KAFKA_SERVER+"'," +
            "'properties.group.id' = '"+groupId+"'," +
            "'scan.startup.mode' = 'latest-offset'," +
            "'format' = 'json'";
        return ddl;
    }
}
