package com.imooc.spark.want;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;

public class HbaseUtil {

    Configuration configuration;
    private static HbaseUtil hbaseUtil;

    private HbaseUtil(){
        configuration  = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");
    }

    public static HbaseUtil getInstance(){
        if (hbaseUtil == null){
            hbaseUtil = new HbaseUtil();
        }
        return hbaseUtil;
    }

    public HTable getTable(String table) {

        HTable hTable = null;
            try {
                hTable = new HTable(configuration,table);
            } catch (IOException e) {
                e.printStackTrace();
            }

        return hTable;
    }

    public void put(String table, String row, String cf, String column, String value){
        HTable hTable = getTable(table);
        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            hTable.put(put);
            System.out.println("put value:"+value +" into rowkey:"+row+ " cf:" + cf +" col:"+column);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long get(String table, String row, String cf, String column){
        HTable hTable = getTable(table);
        Get get = new Get(Bytes.toBytes(row));
        try {
            byte[] res = hTable.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(column));
            return Bytes.toLong(res);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0l;

    }

    public static void main(String[] args){
        String table = "hbase";
        String row = "row1";
        String cf = "cf";
        String column = "test";
        String value = "1000";
        HbaseUtil hbaseUtil = HbaseUtil.getInstance();

//        hbaseUtil.put(table,row,cf,column,value);

        HTable hTable = hbaseUtil.getTable(table);

        try {
            hTable.incrementColumnValue(Bytes.toBytes(row),Bytes.toBytes(cf),Bytes.toBytes(column),1000);
            long res = hbaseUtil.get(table,row,cf,column);
            System.out.println("increment success the result is "+res);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
