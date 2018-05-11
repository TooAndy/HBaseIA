package com.HbaseIA.TwitBase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import utils.Md5Utils;

import static org.apache.hadoop.hbase.CellUtil.cloneValue;

public class TwitsDAO {
    public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
    public static final byte[] TWITS_FAM = Bytes.toBytes("twits");
    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] TWIT_COL = Bytes.toBytes("twit");
    private static final int longLength = 8;   //bytes

    private Connection connection;
    private static final Logger log = Logger.getLogger(TwitsDAO.class.toString());

    public TwitsDAO(Connection connection) {
        this.connection = connection;
    }

    private static byte[] mkRowKey(Twit t) {
        return mkRowKey(t.user, t.dt);
    }

    private static byte[] mkRowKey(String user, DateTime dt) {
        byte[] userHash = Md5Utils.md5sum(user);
        byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
        byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];

        int offset = 0;
        offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
        Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
        return rowKey;
    }

    private static Get mkGet(String user, DateTime dt) {
        Get get = new Get(mkRowKey(user, dt));
        get.addColumn(TWITS_FAM, USER_COL);
        get.addColumn(TWITS_FAM, TWIT_COL);
        return get;
    }

    private static Put mkPut(Twit t) {
        Put put = new Put(mkRowKey(t));
        put.addColumn(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
        put.addColumn(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
        return put;
    }

    private static Scan mkScan(String user) {
        byte[] userHash = Md5Utils.md5sum(user);
        //        byte[] startRow = Bytes.padTail(userHash, longLength);
        byte[] startRow = Bytes.padHead(userHash, longLength);
        byte[] endRow = Bytes.padTail(userHash, longLength);
        endRow[Md5Utils.MD5_LENGTH - 1]++;

        log.log(Level.SEVERE, "Scan starting at: '" + to_str(startRow) + "'");
        log.log(Level.SEVERE, "Scan stopping at: '" + to_str(endRow) + "'");

        Scan s = new Scan(startRow, endRow);
        s.addColumn(TWITS_FAM, USER_COL);
        s.addColumn(TWITS_FAM, TWIT_COL);
        return s;
    }
    private static String to_str(byte[] xs) {
        StringBuilder sb = new StringBuilder(xs.length *2);
        for(byte b : xs) {
            sb.append(b).append(" ");
        }
        sb.deleteCharAt(sb.length() -1);
        return sb.toString();
    }

    private void postTwit(String user, DateTime dt, String text) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put p = mkPut(new Twit(user, dt, text));
        twits.put(p);
        twits.close();
    }

    private com.HbaseIA.TwitBase.model.Twit getTwit(String user, DateTime dt) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get g = mkGet(user, dt);
        Result result = twits.get(g);
        if (result.isEmpty()) {
            return null;
        }

        Twit t = new Twit(result);
        twits.close();
        return t;
    }

    public List<com.HbaseIA.TwitBase.model.Twit> list(String user) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));

        ResultScanner results = twits.getScanner(mkScan(user));
        List<com.HbaseIA.TwitBase.model.Twit> ret = new ArrayList<com.HbaseIA.TwitBase.model.Twit>();
        for (Result r:results){
            ret.add(new Twit(r));
        }
        twits.close();
        return ret;
    }

    private static class Twit extends com.HbaseIA.TwitBase.model.Twit {
        private Twit(Result r) {
            this(cloneValue(r.getColumnLatestCell(TWITS_FAM, USER_COL)),
                    Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength),
                    cloneValue(r.getColumnLatestCell(TWITS_FAM, TWIT_COL)));
        }

        private Twit(String user, DateTime dt, String text) {
            this.user = user;
            this.dt = dt;
            this.text = text;
        }

        private Twit(byte[] user, byte[] dt, byte[] text) {
            this(Bytes.toString(user), new DateTime(Bytes.toLong(dt)), Bytes.toString(text));
        }
    }
}
