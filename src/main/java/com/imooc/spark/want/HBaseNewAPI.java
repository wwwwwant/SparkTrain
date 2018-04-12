package com.imooc.spark.want;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.rdd.JdbcRDD;

import java.io.IOException;

public class HBaseNewAPI {
    public static Admin getAdmin(Connection connection) throws MasterNotRunningException, ZooKeeperConnectionException {
        Admin admin = new HBaseAdmin(connection);
        return admin;
    }

    public static void main(String[] args) {
        String table = "hbase";
        String row = "row1";
        String cf = "cf";
        String column = "test";
        String value = "1000";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");

        Connection conn;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            Table HTable = conn.getTable(TableName.valueOf(table));

            Admin admin = getAdmin(conn);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void increment(Table HTable,String row,String cf,String column,long value){
        try {
            HTable.incrementColumnValue(Bytes.toBytes(row),Bytes.toBytes(cf),Bytes.toBytes(column),value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void delete(Table HTable,String row,String cf,String column){
        Delete delete = new Delete(Bytes.toBytes(row));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column));
        try {
            HTable.delete(delete);
            System.out.println("deleted "+"rowkey:"+row+ " cf:" + cf +" col:"+column);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static int get(Table HTable,String row,String cf,String column) throws IOException {
        Get get = new Get(Bytes.toBytes(row));
        byte[] res = HTable.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(column));
        return Bytes.toInt(res);
    }

    public static void put(Table HTable,String row,String cf,String column, String value){

        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));

        try {
            HTable.put(put);
            System.out.println("put value:"+value +" into rowkey:"+row+ " cf:" + cf +" col:"+column);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
