package com.yang.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.utils.DimUtil;
import com.yang.gmall.realtime.utils.MyThreadpoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
/* *todo:
        Desc: 异步维度关联函数
     *  模板方法设计模式：
     *      在父类中定义完成某一个功能的核心算法骨架，具体的实现细节延迟子类中去实现。
     *      这样子类在不改变核心算法骨架的情况下，每一个子类都可以有自己不同的实现。
 * */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {
    ExecutorService poolInstance;
    String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("---获取线程池对象---");
        poolInstance = MyThreadpoolUtil.getPoolInstance();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        poolInstance.submit(
            new Runnable() {
                @Override
                public void run() {
                    String key = getKey(t);
                    JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                    //因为关联的维度表各种各样，所以这里只提供程序关联的模板，具体的字段合并交给另外的方法
                    if (dimInfo!=null){
                        join(t,dimInfo);
                    }
                    resultFuture.complete(Collections.singleton(t));
                }
            }
        );
    }
    /*// T obj  代表流中的一条数据
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //当流中的数据过来之后，会从线程池中获取一个线程，并且执行线程的run方法
        executorService.submit(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        long start = System.currentTimeMillis();
                        //获取要关联的维度的key
                        String key = getKey(obj);
                        //获取维度数据
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        if (dimInfoJsonObj != null) {
                            //维度关联 将维度的值 赋给对象
                            join(obj, dimInfoJsonObj);
                        }
                        long end = System.currentTimeMillis();
                        System.out.println("异步查询维度"+tableName+"耗时"+(end-start)+"毫秒");
                        resultFuture.complete(Collections.singleton(obj));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("异步查询维度失败");
                    }
                }
            }
        );
    }*/
    public abstract String getKey(T t);
    public abstract void join(T t, JSONObject dimInfoJsonObj);
}
