package com.zqw.demo;

import org.apache.commons.digester.ObjectCreateRule;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import javax.xml.soap.Text;
import java.io.IOException;
import java.util.List;

public class TestZookeeper {

//    private String connectString = "hadoop21:2181,hadoop22:2181,hadoop23:2181";
    private static String connectString = "hadoop24:2181";
    private int sessionTimeout = 2000;

    private ZooKeeper zkClient;

    //创建客户端
    @Before
    public void test1() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, (event) -> {
            System.out.println(event.getType() + "\t" + event.getPath());
        });

    }

    //创建节点
    @Test
    public void test2() throws KeeperException, InterruptedException {
        String path = zkClient.create("/test_service", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);

    }

    //获取子节点
    @Test
    public void test3() throws KeeperException, InterruptedException {
        //参数2：是否监听
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        //监听节点的变化
        Thread.sleep(Long.MAX_VALUE);
    }

    //判断节点是否存在
    @Test
    public void test4() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/wendy", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
