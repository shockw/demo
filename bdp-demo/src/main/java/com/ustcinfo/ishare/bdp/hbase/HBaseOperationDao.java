package com.ustcinfo.ishare.bdp.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Describe:
 * Author: Nee
 * Mail: ni.liang@ustcinfo.com
 * Date: 2018/8/1
 * Copyright: © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 * reserved.
 */
public class HBaseOperationDao {
    private static Logger logger = LoggerFactory.getLogger(HBaseOperationDao.class);

    /*集群配置对象*/
    private static Configuration conf = null;

    /*集群连接对象*/
    private static Connection connection = null;

    /*hbase集群管理对象*/
    private static Admin admin = null;

    /*hbase replication管理对象*/
    private static ReplicationAdmin replicationAdmin = null;

    /**
     * 初始化
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "node56,node221,node227");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            replicationAdmin = new ReplicationAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 全表扫描
     * @param name
     * @param family
     * @return
     */
    public static List<Result> scanTable(String name, String family) {
        TableName tableName = TableName.valueOf(name);
        List<Result> results = new ArrayList<>();
        Scan scan = new Scan();
        if (family != null) {
            scan.addFamily(Bytes.toBytes(family));
        }
        try (Table table = connection.getTable(tableName)) {
            ResultScanner resultScanner = table.getScanner(scan);
            for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext(); ) {
                Result result = iterator.next();
                results.add(result);
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String rowKeyName = new String(CellUtil.cloneRow(cell));
                    String colFamilyName = new String(CellUtil.cloneFamily(cell));
                    String qualifier = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    logger.info("rowKey:{},colFamily:{},qualifier:{},value:{}", rowKeyName, colFamilyName, qualifier, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /**
     * 单表查询
     * @param name
     * @param rowKey
     * @return
     */
    public static Result get(String name, String rowKey) {
        TableName tableName = TableName.valueOf(name);
        Result result = null;
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(rowKey.getBytes());
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 单列簇查询
     * @param name
     * @param rowKeyName
     * @param familyName
     * @return
     */
    public static List<Cell> getCol(String name, String rowKeyName, String familyName) {
        TableName tableName = TableName.valueOf(name);
        byte[] rowKey = Bytes.toBytes(rowKeyName);
        byte[] colFamily = Bytes.toBytes(familyName);
        List<Cell> cells = null;
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(rowKey);
            get.addFamily(colFamily);
            Result result = table.get(get);
            cells = result.listCells();
            for (Cell cell : cells) {
                String colFamilyName = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                logger.info("rowKey:{},colFamily:{},qualifier:{},value:{}", rowKeyName, colFamilyName, qualifier, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cells;
    }

    /**
     * 批量插入
     * @param name
     * @param puts
     */
    public static void putList(String name, List<Put> puts) {
        TableName tableName = TableName.valueOf(name);
        try (Table table = connection.getTable(tableName)) {
            for (Put put : puts) {
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条put
     * @param name
     * @param put
     */
    public static void put(String name, Put put) {
        TableName tableName = TableName.valueOf(name);
        try (Table table = connection.getTable(tableName)) {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单列删除
     * @param name
     * @param rowKeyStr
     * @param familyName
     * @param qualifierName
     */
    public static void delete(String name, String rowKeyStr, String familyName, String qualifierName) {
        TableName tableName = TableName.valueOf(name);
        byte[] rowKey = Bytes.toBytes(rowKeyStr);
        byte[] family = Bytes.toBytes(familyName);
        byte[] colName = Bytes.toBytes(qualifierName);
        Delete delete = new Delete(rowKey);
        /*删除某个版本的*/
        delete.addColumn(family, colName);
        /*删除所有版本*/
        delete.addColumns(family, colName);
        /*删除整个列簇的数据*/
        delete.addFamily(family);
        try (Table table = connection.getTable(tableName)) {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 整行删除
     * @param name
     * @param rowKeyStr
     */
    public static void deleteAll(String name, String rowKeyStr) {
        TableName tableName = TableName.valueOf(name);
        byte[] rowKey = Bytes.toBytes(rowKeyStr);
        Delete delete = new Delete(rowKey);
        try (Table table = connection.getTable(tableName)) {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        scanTable("test", null);
        Put put = new Put("123".getBytes());
        put.addColumn("family1".getBytes(), "age".getBytes(), "123".getBytes());
        put.addColumn("family1".getBytes(), "name".getBytes(), "123".getBytes());
        
       
        put("test4", put);
//        deleteAll("test4", "123");
//        delete("test4", "123", "family1", "name");
//        System.out.println(get("test4", "1"));
//        System.out.println(getCol("test4", "1", "family1"));
    }
}
