package com.zqw.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HBaseDemo1 {


    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    /**
     * 判断表是否存在
     * @return
     */
    public static boolean isExist(String tableName) throws IOException{
        //老API
//        HBaseAdmin admin = new HBaseAdmin(conf);

        //新API
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(isExist(tableName)){
            System.out.println("表已经存在");
        }
        //获取表描述器
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }

        admin.createTable(htd);
        System.out.println("表创建成功");
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        if(isExist(tableName)){
            if(!admin.isTableDisabled(tn)){
                //先卸载表
                admin.disableTable(tn);
            }
            //删除表
            admin.deleteTable(tn);
            System.out.println("表删除成功");
        }else {
            System.out.println("表不存在");
        }
    }


    /**
     * 添加数据
     * cf : 列族
     */
    public static void addRow(String tableName, String rowKey, String cf, String column, String vlaue) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        //创建Table对象，用于管理表中的数据
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建Put对象，用于封装上传数据
        Put put = new Put(Bytes.toBytes(rowKey));

        //封装数据，列族:列名，数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(vlaue));
        //上传数据
        table.put(put);
    }

    /**
     * 删除一行数据
     */
    public static void deleteRow(String tableName, String rowKey, String cf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        table.delete(new Delete(Bytes.toBytes(rowKey)));
    }
    /**
     * 删除多行数据
     */
    public static void deleteMultiRow(String tableName, String... rowKeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        //批量删除
        List<Delete> list = new ArrayList<>();
        for (String row : rowKeys){
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
    }

    /**
     * 扫描数据
     */
    public static void getAllRows(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        //扫描所有的版本
//        scan.setMaxVersions();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();

            for(Cell cell : cells){
                System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列名" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    /**
     * 得到一个具体的数据
     */
    public static void getRows(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        //扫描所有的版本
//        scan.setMaxVersions();
        //指定列族
//        get.addFamily(Bytes.toBytes("info1"));
        //指定列
//        get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
        Result result = table.get(get);

        Cell[] cells = result.rawCells();

        for(Cell cell : cells){
            System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列名" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

    }

    //测试
    public static void main(String[] args) throws IOException {
//        System.out.println(isExist("staff"));

        //创建表
//        createTable("staff", "info1", "info2");

        //删除表
//        deleteTable("staff");

        //上传数据
//        addRow("staff", "1001", "info1", "name", "aaa");

        //删除一行数据
//        deleteRow("staff", "1002", null);

        //删除多行数据
//        deleteMultiRow("staff", "1002", "1001");

        getAllRows("staff");
    }
}
