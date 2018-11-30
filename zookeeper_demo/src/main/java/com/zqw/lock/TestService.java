package com.zqw.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 模拟 zookeeper实现分布式锁
 */
public class TestService implements Watcher{

    private ZooKeeper zk;



    private int sessionTimeout = 2000;

    //zookeeper 节点路径
    private final String PATH = "/test_service";
    //节点内容
    private String nodeData = "aaa";
    //本服务节点名
    private String myNode = null;

    public TestService(String config, String lockName) throws Exception {

        //1. 创建连接
        zk = new ZooKeeper(config, sessionTimeout,this);

        //创建节点信息
        Stat stat = zk.exists(PATH, false);
        if (stat == null) {
            // 如果根节点不存在，则创建根节点
            zk.create(PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }


    //业务逻辑
    public void doSomething() throws InterruptedException {
        System.out.println("开始做一些业务");
        Thread.sleep(5000);
    }

    //获取所有节点列表
    public String getMinNodeNum() throws Exception {
        //获取所有节点的列表
        List<String> serviceList = zk.getChildren(PATH, true);

        System.out.println(serviceList);
        System.out.println("------------------");

        Optional<Long> aaa = serviceList.stream()
                .map((x) -> Long.parseLong(x.replace("/test_service/aaa", "")))
                .min(Long::compare);

        Long minNodeNum = null;
        if (aaa.isPresent()) {
            minNodeNum = aaa.get();
        }
        return "/test_service/aaa" + minNodeNum;
    }


    //创建节点
    public void createNode() throws KeeperException, InterruptedException {

        String nodeData = "aaa";

        //创建有序列的临时节点
        myNode = zk.create(PATH, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("返回创建的节点路径" + myNode);
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            //获取序列最小的节点
            String minNodeNum = getMinNodeNum();
            System.out.println("监听启动，最小的节点为："+minNodeNum);
            if(myNode != null){
                if(myNode.equals(minNodeNum)){
                    //获得锁
                    doSomething();
                }else{
                    //继续等待
                    //?
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}