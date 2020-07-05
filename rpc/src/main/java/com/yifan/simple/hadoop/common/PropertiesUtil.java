package com.yifan.simple.hadoop.common;

import java.io.*;
import java.util.Properties;

/**
 * 一个配置文件工具类
 **/
public class PropertiesUtil {

    public static void main(String[] args) {
        System.out.println(getSubClass("HelloService"));
    }

    // 服务名称， 服务实现类
    private static Properties properties = null;
    private static final String DEFAULT_ENCODING = "UTF-8";

    static {
        properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream("service.properties");
            if (inputStream == null) {
                System.err.println("Sorry, unable to find service.properties");
            }
            properties.load(new InputStreamReader(inputStream, DEFAULT_ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取实现类
     */
    public static String getSubClass(String serviceName) {
        return properties.getProperty(serviceName);
    }
}
