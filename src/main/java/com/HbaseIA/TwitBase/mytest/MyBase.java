package com.HbaseIA.TwitBase.mytest;

import com.HbaseIA.TwitBase.common.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class MyBase {
    static Configuration configuration = HBaseConfiguration.create();

    /**
     * 创建Hbase中的表
     *
     * @param connection   链接
     * @param tableName    表名
     * @param columnFamily 列簇名
     *
     * @throws IOException
     */
    public static void createTable(Connection connection, TableName tableName, String[] columnFamily) throws IOException {
        System.out.println("创建表");
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i<columnFamily.length; ++i) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("创建表成功");
        }
    }

    public static void delectTable(Connection connection, TableName tableName) throws IOException {
        System.out.println("删除表");
        MyBase.disableTable(connection, tableName);

        Admin admin = connection.getAdmin();
        admin.deleteTable(tableName);
        System.out.println("delete table success");
    }

    public static void disableTable(Connection connection, TableName tableName) throws IOException {
        System.out.println("disable table");
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            System.out.println("table not exists");
            return;
        } else if (admin.isTableDisabled(tableName)) {
            System.out.println("table " + tableName.toString() + " was already disabled");
            return;
        }
        admin.disableTable(tableName);
        System.out.println("disable table " + tableName.toString());
    }

    public static void scanTable(Connection connection, TableName tableName) throws IOException {
        System.out.println("scanTable " + tableName.toString());
        if (!connection.getAdmin().tableExists(tableName)) {
            System.out.println("table " + tableName.toString() + "is not exists");
            return;
        }

        Scan scan = new Scan();
        Table table = connection.getTable(tableName);
        ResultScanner resultScanner = table.getScanner(scan);
        try {
            for (Result r : resultScanner) {
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()) {
                    System.out.print(Bytes.toString(r.getRow()) + ":");
                    System.out.print(Bytes.toString(entry.getKey()) + "#");
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
                    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
                        System.out.print(Bytes.toString(en.getKey()) + "##");
                        NavigableMap<Long, byte[]> ma = en.getValue();
                        for (Map.Entry<Long, byte[]> e : ma.entrySet()) {
                            System.out.print(e.getKey() + "###");
                            System.out.println(Bytes.toString(e.getValue()));
                        }
                    }
                }
                //                System.out.println(r);
                //                System.out.println("\n-----------------------------------------------------\n");

            }
        } finally {
            resultScanner.close();
        }
    }


    public static void main(String[] args) throws IOException {

        configuration.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty("zookeeper.port"));
        configuration.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty("zookeeper.quorum"));
        
        Connection     connection = ConnectionFactory.createConnection(configuration);

        //        MyBase.createTable(connection, TableName.valueOf("wahaha"), new String[]{"cf"});
        //        MyBase.delectTable(connection, TableName.valueOf("users"));
        //        MyBase.delectTable(connection, TableName.valueOf("users1"));
        try {
            //            MyBase.scanTable(connection, TableName.valueOf("usertable"));
            Table table = connection.getTable(TableName.valueOf("users"));
            try {
//                Put put = new Put(Bytes.toBytes("TheRealMT"));

                //添加数据
                //put.add已经过期,使用addColumn
                //                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Mike Twain"));
                //                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes("aniss@qq.com"));
                //                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));
                //                //提交添加的数据
                //                table.put(put);

                //修改数据
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes("Strong4Password"));
//                table.put(put);

                //读取数据
                Get get = new Get(Bytes.toBytes("TheRealMT"));
                get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password")).setTimeStamp(1525915443596L);
                Result result = table.get(get);
                byte[] passwd = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"));
                System.out.println("Password is: " + Bytes.toString(passwd));

//                List<Cell> passwords = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("password"));
//                for (Cell pa : passwords){
//                    System.out.println(pa);
//                }
//                passwd = passwords.get(0).getValue();
//                String currentPassword = Bytes.toString(passwd);
//                System.out.println("currentPassword: " + currentPassword);
//                passwd = passwords.get(1).getValue();
//                System.out.println("oldPassword: " + passwd.toString());


                //                // 删除数据
                //                Delete delete = new Delete(Bytes.toBytes("TheRealMT"));
                //                delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
                //                table.delete(delete);


                //                byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("Qualifier"));
                //                String valueStr = Bytes.toString(value);
                //                System.out.println("GET: " + valueStr);

                //                Scan scan = new Scan();
                //                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Qualifier"));
                //                ResultScanner scanner = table.getScanner(scan);
                //                try {
                //                    for (Result rr : scanner) {
                //                        System.out.println("Found row: " + rr);
                //                    }
                //                } finally {
                //                    scanner.close();
                //                }

            } finally {
                if (table != null) table.close();
            }
        } finally {
            connection.close();
        }

    }
}
