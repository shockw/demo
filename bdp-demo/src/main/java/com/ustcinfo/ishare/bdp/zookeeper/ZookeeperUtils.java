package com.ustcinfo.ishare.bdp.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.yarn.lib.ZKClient;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Describe:
 * Author: Nee
 * Mail: ni.liang@ustcinfo.com
 * Date: 2018/7/30
 * Copyright: © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 * reserved.
 */
public class ZookeeperUtils {

    public static ZkClient zkClient = null;

//    初始化
    static {
        zkClient = new ZkClient("node86:2181,node99:2181,node101:2181", 5000);
    }

    /**
     * 获取所有子节点
     * @param path
     * @return
     */
    public static List<String> listPath(String path) {
        return zkClient.getChildren(path);
    }

    /**
     * @param path
     * @param data
     * @param createMode EPHEMERAL:非持久化、PERSISTENT：持久化
     */
    public static void createPath(String path, String data, CreateMode createMode) {
        zkClient.create(path, data, createMode);
    }

    /**
     * 删除节点
     * @param path
     */
    public static void deletePath(String path) {
        zkClient.delete(path);
    }

    /**
     * 获取节点数据
     * @param path
     * @return
     */
    public static String get(String path) {
        return zkClient.readData(path);
    }

    public static void main(String[] args) {
        System.out.println(listPath("/"));
        System.out.println(get("/"));
    }

}
