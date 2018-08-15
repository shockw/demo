package com.ustcinfo.ishare.bdp.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Describe:HBase 常用操作
 * Author: Nee
 * Mail: ni.liang@ustcinfo.com
 * Date: 2018/7/30
 * Copyright: © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 * reserved.
 */
public class HBaseMetaDao {

    private static Logger logger = LoggerFactory.getLogger(HBaseMetaDao.class);

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
     * 获取所有表
     * @return
     */
    public static List<String> listTables() {
        List<String> tables = new ArrayList<>();
        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                tables.add(new String(tableName.getName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tables;
    }

    /**
     * 判断表是否存在
     * @param name
     * @return
     */
    public static boolean tableExist(String name) {
        try {
            return admin.tableExists(TableName.valueOf(name));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建表
     * @param name
     * @param families
     */
    public static void createTable(String name, String... families) {
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        if (!tableExist(name)) {
            for (String family : families) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            try {
                admin.createTable(hTableDescriptor);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除表
     * @param name
     */
    public static void deleteTable(String name) {
        TableName tableName = TableName.valueOf(name);
        try {
            if (tableExist(name))
                if (!admin.isTableDisabled(tableName)) {
                    admin.disableTable(tableName);
                }
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 禁用表
     * @param name
     */
    public static void disableTable(String name) {
        TableName tableName = TableName.valueOf(name);
        if (tableExist(name)) {
            try {
                admin.disableTable(tableName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * 启用表
     * @param name
     */
    public static void enableTable(String name) {
        TableName tableName = TableName.valueOf(name);
        if (tableExist(name)) {
            try {
                admin.enableTable(tableName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 清除表所有数据
     * @param name
     * @param preserveSplits
     */
    public static void truncateTable(String name, boolean preserveSplits) {
        TableName tableName = TableName.valueOf(name);
        if (tableExist(name)) {
            try {
                admin.truncateTable(tableName, preserveSplits);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 添加一个hbase同步集群,对主集群的put/delete操作将会同步到该集群中
     *
     * @param id         同步集群的唯一性标识
     * @param clusterKey 同步集群地址,格式为"zk_ip:zk_port:zk_znode",例如
     *                   "192.168.80.99:2181:/hbase"
     * @param tableCfs   选择需要同步的表及其列簇,表名作为key,列簇名构成一集合;该参数可以为空
     * @throws ReplicationException
     */
    public static void addPeer(String id, String clusterKey, Map<TableName, ? extends Collection<String>> tableCfs)
            throws ReplicationException {
        addPeer(id, new ReplicationPeerConfig().setClusterKey(clusterKey), tableCfs);
    }

    public static void addPeer(String id, ReplicationPeerConfig peerConfig,
                               Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException {
        replicationAdmin.addPeer(id, peerConfig, tableCfs);
    }

    /**
     * 显示当前集群的复制关系 *
     */
    public static void listPeers() {
        Map<String, ReplicationPeerConfig> replicationConfig = replicationAdmin.listPeerConfigs();
        Set<String> set = replicationConfig.keySet();
        for (String s : set) {
            logger.info(s + "/" + replicationConfig.get(s));
        }
    }

    /**
     * 删除复制关系
     *
     * @param id 从集群id
     * @throws ReplicationException
     */
    public static void removePeer(String id) throws ReplicationException {
        replicationAdmin.removePeer(id);
    }

    /**
     * 添加peer下的表、列族复制关系
     *
     * @param id       从集群id
     * @param tableCfs 选择需要同步的表及其列簇,表名作为key,列簇名为一集合
     * @throws ReplicationException
     */
    public static void addPeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
            throws ReplicationException {
        replicationAdmin.appendPeerTableCFs(id, tableCfs);
    }


    public static void main(String[] args) {
        System.out.println(listTables());
        System.out.println(tableExist("t1"));
        createTable("ceshi", "f1", "f2");
        deleteTable("ceshi");

    }
}
