package com.HbaseIA.TwitBase.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;

import java.io.IOException;

public class MyBaseTest {
    static Configuration configuration = HBaseConfiguration.create();
    Connection connection = null;
    @Before
    public void setUp() throws IOException {
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.104.2.219");
        connection = ConnectionFactory.createConnection(configuration);
    }

    public void createTable(Connection connection, TableName tableName, String[] columnFamily) throws IOException {
        System.out.println("创建表");
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i=0; i<columnFamily.length; ++i){
                tableDesc.addFamily(new HColumnDescriptor(columnFamily[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("创建表成功");
        }
    }
}
