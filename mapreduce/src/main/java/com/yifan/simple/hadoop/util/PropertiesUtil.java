package com.yifan.simple.hadoop.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertiesUtil {
    // 服务名称， 服务实现类
    public static Properties loadConf() {
        String DEFAULT_ENCODING = "UTF-8";
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new BufferedInputStream(PropertiesUtil.class.getClassLoader().getResourceAsStream("nxcompute.properties"));
            properties.load(new InputStreamReader(inputStream, DEFAULT_ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
