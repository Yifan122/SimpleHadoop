package com.yifan.simple.hadoop.mapper;

import com.yifan.simple.hadoop.inputformat.MapInputFormat;
import com.yifan.simple.hadoop.outputformat.MapOutputFormat;

public class Mapper {

    /**
     * 初始化方法
     */
    protected void setup(MapInputFormat mapInputFormat, MapOutputFormat mapOutputFormat) {
    }

    /**
     * 一条记录处理一次的映射方法
     * 方法的参数接受：currentKey  currentValue 一种key-value的形式
     * 执行方法的逻辑
     * 输出结果
     */
    protected void map(Object currentKey, Object currentValue, MapOutputFormat mapOutputFormat) {

        // 编写业务逻辑
        String value = (String) currentValue;
        String[] words = value.split(" ");

        for (String word : words) {
            mapOutputFormat.write(word, 1);
        }
    }

    /**
     * 回收方法
     */
    protected void cleanup(MapInputFormat mapInputFormat, MapOutputFormat mapOutputFormat) {

        mapOutputFormat.close();
    }

    /**
     * 核心数据处理流转方法
     */
    public void run(MapInputFormat mapInputFormat, MapOutputFormat mapOutputFormat) {

        // 在map方法被正式调用之前，先执行一次setup方法。
        setup(mapInputFormat, mapOutputFormat);

        try {

            // 使用nextKeyValue方法判断是否能取到下一个key-value对
            while (mapInputFormat.nextKeyValue()) {

                // 如果取到key-value对， 就调用map方法执行一次处理
                map(mapInputFormat.getCurrentKey(), mapInputFormat.getCurrentValue(), mapOutputFormat);
            }

        } finally {

            // run方法的最后，执行cleanup方法进行程序的收尾， 只会被调用一次
            cleanup(mapInputFormat, mapOutputFormat);
        }
    }
}
