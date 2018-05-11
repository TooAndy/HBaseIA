package com.HbaseIA.TwitBase.model;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDAO {
    public static final TableName TABLE_NAME = TableName.valueOf("users");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");
    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] NAME_COL = Bytes.toBytes("name");
    public static final byte[] EMAIL_COL = Bytes.toBytes("email");
    public static final byte[] PASS_COL = Bytes.toBytes("password");
    public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");
    private Connection connection;

    public UsersDAO(Connection connection) {
        this.connection = connection;
    }

    public static class User extends com.HbaseIA.TwitBase.model.User {
        private User(String user, String name, String email, String password) {
            this.user = user;
            this.name = name;
            this.email = email;
            this.password = password;
        }

        private User(byte[] user, byte[] name, byte[] email, byte[] password) {
            this(Bytes.toString(user), Bytes.toString(name), Bytes.toString(email), Bytes.toString(password));
        }

        private User(Result r) {
            this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL),
                    r.getValue(INFO_FAM, EMAIL_COL), r.getValue(INFO_FAM, PASS_COL));
        }
    }

    private static Get mkGet(String user) {
        Get g = new Get(Bytes.toBytes(user));
        g.addFamily(INFO_FAM);
        return g;
    }


    public static Put mkPut(User u) {
        Put p = new Put(Bytes.toBytes(u.user));
        p.addColumn(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
        p.addColumn(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
        p.addColumn(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
        p.addColumn(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
        return p;
    }

    public static Delete mkDel(String user) {
        Delete delete = new Delete(Bytes.toBytes(user));
        return delete;
    }

    public static Scan mkScan() {
        Scan scan = new Scan();
        scan.addFamily(INFO_FAM);
        return scan;
    }

    public static Scan mkScan(String begin, String end) {
        Scan scan = new Scan(Bytes.toBytes(begin), Bytes.toBytes(end));
        scan.addFamily(INFO_FAM);
        return scan;
    }

    public void addUser(String user, String name, String email, String password) throws IOException {
        Table users = connection.getTable(TABLE_NAME);
        Put p = mkPut(new User(user, name, email, password));
        users.put(p);
        users.close();
    }

    public com.HbaseIA.TwitBase.model.User getUser(String user) throws IOException {
        Table users = connection.getTable(TABLE_NAME);
        Get g = mkGet(user);
        Result result = users.get(g);
        if (result.isEmpty()) {
            return null;
        }

        User u = new User(result);
        users.close();
        return u;
    }

    public List<com.HbaseIA.TwitBase.model.User> getUsers() throws IOException {
        Table users = connection.getTable(TABLE_NAME);
        ResultScanner results = users.getScanner(mkScan());
        ArrayList<com.HbaseIA.TwitBase.model.User> ret = new ArrayList<com.HbaseIA.TwitBase.model.User>();
        for (Result r : results) {
            ret.add(new User(r));
        }
        users.close();
        return ret;
    }

    public List<com.HbaseIA.TwitBase.model.User> getUsers(String begin, String end) throws IOException {
        Table users = connection.getTable(TABLE_NAME);
        ResultScanner results = users.getScanner(mkScan(begin, end));
        ArrayList<com.HbaseIA.TwitBase.model.User> ret = new ArrayList<com.HbaseIA.TwitBase.model.User>();
        for (Result r : results) {
            ret.add(new User(r));
        }
        users.close();
        return ret;
    }

    public void deleteUser(String user) throws IOException {
        Table users = connection.getTable(TABLE_NAME);
        Delete d = mkDel(user);
        users.delete(d);
        users.close();
    }
}
