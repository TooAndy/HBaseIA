package com.HbaseIA.TwitBase.common;

/**
 * @author Andre Wei
 * create time: 2018年9月20日  15:53:42
 */
public class Const {
    public static String  ZK_QUORUM = ConfigurationManager.getProperty("zookeeper.quorum");
    public static String ZK_PORT = ConfigurationManager.getProperty("zookeeper.port");
}
