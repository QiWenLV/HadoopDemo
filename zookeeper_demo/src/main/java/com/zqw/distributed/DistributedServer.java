package com.zqw.distributed;

import org.apache.zookeeper.*;

/**
 * 服务动态上下线，服务端
 */
public class DistributedServer {

    private static ZooKeeper zk;

    public static void  getConnect() throws Exception{
        zk = new ZooKeeper("hadoop24:2181,hadoop24:2182", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>");
            }
        });
    }

    public static void  registerServer(String hostAndPort)throws  Exception{
        zk.create("/servers/server",hostAndPort.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    }

    public static void  handleService(String hostAndPort)throws Exception{

        System.out.println(hostAndPort+"start working");

        Thread.sleep(Long.MAX_VALUE);
        zk.close();
    }


    public static void main(String[] args) throws  Exception{
        //连接zookeeper
        getConnect();
        //注册服务
        registerServer(args[0]);

        //处理业务
        handleService(args[0]);
    }
}
