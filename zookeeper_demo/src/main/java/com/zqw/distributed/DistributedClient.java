package com.zqw.distributed;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

public class DistributedClient {
    private static ZooKeeper zk;

    //获得连接
    public static void  getConnect() throws Exception{
        zk = new ZooKeeper("hadoop24:2181,hadoop24:2182", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        });
    }

    public static void getServerList()throws  Exception{

        List<String> children = zk.getChildren("/servers", true);
        List<String> servers = new ArrayList<String>();
        for (String child : children) {

            byte[] data = zk.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }

        System.out.println(servers);


    }


    public static  void handleServer() throws  Exception{
        System.out.println("客户端开始工作了");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws  Exception {
        //连接zookeeper
        getConnect();
        //获取服务列表
        getServerList();
        //处理业务
        handleServer();
    }
}
