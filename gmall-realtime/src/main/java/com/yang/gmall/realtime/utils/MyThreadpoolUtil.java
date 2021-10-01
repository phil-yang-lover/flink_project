package com.yang.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyThreadpoolUtil {
    private static ThreadPoolExecutor pool;
    public static ThreadPoolExecutor getPoolInstance(){
        if (pool==null){
            synchronized (MyThreadpoolUtil.class){
                if (pool==null){
                    pool = new ThreadPoolExecutor(
                        4,
                        20,
                        10,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
