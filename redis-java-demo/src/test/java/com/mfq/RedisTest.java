package com.mfq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisTest {
    private static RedisClient redisClient;

    private static StatefulRedisConnection<String, String> connection;

    private static RedisAsyncCommands<String, String> asyncCommands;

    @Before
    public void before(){
        redisClient = RedisClient.create("redis://127.0.0.1:6379/0");
        connection = redisClient.connect();
        asyncCommands = connection.async();
        // RedisCommands<String, String> syncCommands = connection.sync();
    }

    @After
    public void after(){
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testCacheProduct() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        Product product = new Product();
        product.setName("杯子");
        product.setPrice(100d);
        product.setDesc("这是一个杯子");
        String json = objectMapper.writeValueAsString(product);

        asyncCommands.set("product", json).get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testGetProduct() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = asyncCommands.get("product").get(1, TimeUnit.SECONDS);
        Product product = objectMapper.readValue(json, new TypeReference<>() {
        });
        System.out.println(product);
    }

    @Test
    public void testLock() throws Exception {
        int threadNum = 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        Runnable runnable = () -> {
            try {
                countDownLatch.await();
                while (true) {
                    // 获取锁
                    SetArgs setArgs = SetArgs.Builder.ex(5).nx();
                    String succ = asyncCommands.set("update-product",
                            Thread.currentThread().getName(), setArgs).get(1, TimeUnit.SECONDS);
                    // 加锁失败
                    if (!"OK".equals(succ)) {
                        System.out.println(Thread.currentThread().getName() + "加锁失败，自选等待锁");
                        Thread.sleep(100);
                    } else {
                        System.out.println(Thread.currentThread().getName() + "加锁成功");
                        break;
                    }
                }
                // 加锁成功
                System.out.println(Thread.currentThread().getName() + "开始执行业务逻辑");
                Thread.sleep(1000);
                System.out.println(Thread.currentThread().getName() + "完成业务逻辑");
                // 释放锁
                asyncCommands.del("update-product").get(1, TimeUnit.SECONDS);
                System.out.println(Thread.currentThread().getName() + "释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        Thread thread1 = new Thread(runnable);
        Thread thread2 = new Thread(runnable);
        Thread thread3 = new Thread(runnable);
        thread1.start();
        thread2.start();
        thread3.start();
        countDownLatch.countDown();

        Thread.sleep(TimeUnit.DAYS.toMillis(1));
    }
}
