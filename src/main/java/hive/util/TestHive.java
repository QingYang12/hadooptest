package hive.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *  java jdbc操作hive
 */
public class TestHive {
    public static void main(String[] args) throws Exception {
            //测试方法
        BasicConfigurator.configure();//开启日志
        //加载hive驱动
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        //连接hive数据库
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.31.174:10000/hivetest","hive",null);
        String sql = "select * from person";
        PreparedStatement pstm = conn.prepareStatement(sql);
        ResultSet rs = pstm.executeQuery();
        while(rs.next()){
            String name = rs.getString("name");
            int age = rs.getInt("age");
            System.out.println(name+":"+age);
        }
        rs.close();
        pstm.close();
        conn.close();
    }

}
