package com.yang.gmall.realtime.utils;
//TODO : 获取Jedis客户端工具类
import com.google.inject.internal.util.$SourceProvider;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyRedisUtil {
    private static JedisPool jedisPool = null;
    public static Jedis getJedis(){
        if (jedisPool==null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMaxTotal(20);
            jedisPool = new JedisPool(jedisPoolConfig,"hadoop2102",6379,10000);
        }
        System.out.println("开辟连接池");
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }
}
