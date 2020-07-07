package com.yifan.simple.hadoop.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 一个配置文件工具类
 **/
public class PropertiesUtil {

    private static final String DEFAULT_ENCODING = "UTF-8";
    // 服务名称， 服务实现类
    private static Properties properties = null;

    static {
        properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new BufferedInputStream(PropertiesUtil.class.getClassLoader().getResourceAsStream("server.properties"));
            properties.load(new InputStreamReader(inputStream, DEFAULT_ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取实现类
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
