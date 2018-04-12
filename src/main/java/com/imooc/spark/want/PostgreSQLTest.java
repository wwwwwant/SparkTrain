package com.imooc.spark.want;

import java.sql.*;

public class PostgreSQLTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.postgresql.Driver");

        Connection conn = DriverManager.getConnection("jdbc:postgresql://pgm-uf6l3z5ge32t5h4neo.pg.rds.aliyuncs.com:3432/","fdadmin","fd2018");


        PreparedStatement pst;


        String sql = "select * from wordcount";

        pst = conn.prepareStatement(sql);
        ResultSet resultSet = pst.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString("word"));
        }







    }
}
