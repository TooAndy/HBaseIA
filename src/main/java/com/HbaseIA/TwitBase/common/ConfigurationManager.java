package com.HbaseIA.TwitBase.common;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author Andre Wei
 * create time: 2018/9/20 14:01
 */
public class GetProperties {
    static {
        Properties properties = new Properties();
        InputStream in = GetProperties.class.getClassLoader().getResourceAsStream("config/config.properties");
    }
}
