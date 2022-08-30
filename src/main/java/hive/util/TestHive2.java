package hive.util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

/**
 * java jdbc操作hive
 */
public class TestHive2 {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.31.174:10000/hivetest";
    private static String user = "hive";
    private static String password = null;
    private static String sql="";
    private static ResultSet res;

    private static Connection conn;
    private static Statement stmt;

    // 加载驱动、创建连接
    @Before
    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        conn= DriverManager.getConnection(url,user,password);
        stmt=conn.createStatement();
    }
    @Test
    public  void createDatabase() throws SQLException {
        sql = "create database hive_jdbc";
        System.out.println("Running:"+ sql);
        stmt.execute(sql);
    }

    // 查询所有数据库
    @Test
    public void showDatabases() throws SQLException {
        sql="show databases";
        System.out.println("Running:"+sql);
        res = stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString(1));
        }
    }


    //创建表
    @Test
    public void createTable() throws SQLException {
        sql= "create table emp(id int,name string,age int) row format delimited fields terminated by ','";
        System.out.println("Running:"+sql);
        stmt.execute(sql);
    }


    //查看表结构
    @Test
    public void descTable() throws SQLException {
        sql ="desc emp";
        System.out.println("Running:"+sql);
        res = stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString(1)+"\t"+res.getString(2));
        }
    }


    //加载数据
    @Test
    public void loadData() throws SQLException {
        String filePath = "'/opt/emp.test'";
        sql="load data local inpath "+filePath+" "+"into table emp";
        System.out.println("Running:"+sql);
        stmt.execute(sql);
    }

    //查询数据
    @Test
    public void selectData() throws SQLException {
        sql = "select * from emp";
        System.out.println("Running:"+sql);
        res = stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString("id") + "\t\t" + res.getString("name") + "\t\t" + res.getString("age"));
        }
    }



    // 统计查询（会运行mapreduce作业）
    @Test
    public void countData() throws Exception {
        sql = "select count(1) from emp";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getInt(1) );
        }
    }

    // 删除数据库
    @Test
    public void dropDatabase() throws Exception {
        String sql = "drop database if exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 删除数据库表
    @Test
    public void deopTable() throws Exception {
        String sql = "drop table if exists emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 释放资源
    @After
    public void destory() throws Exception {
        if ( res != null) {
            res.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
