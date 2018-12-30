package com.zqw.wordcount;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 驱动类，用来提交任务
 */
public class WordCountTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //创建拓扑
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置spout，获取数据
        topologyBuilder.setSpout("readfileSpout", new ReadFileSpout());

        //设置splitBolt，对句子进行切割(连接方式，随机读取)
        topologyBuilder.setBolt("splitBolt", new SplitBolt()).shuffleGrouping("readfileSpout");

        //设置wordcountBolt，对单词进行统计
        topologyBuilder.setBolt("wordcountBolt", new WordCountBolt()).shuffleGrouping("splitBolt");


        //准备一个配置文件
        Config config = new Config();
        //设置worker的并行度
        config.setNumWorkers(2);

        //任务提交的两种方式 ：

        if (args != null && args.length > 0) {
            //      集群模式
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        } else {
            //      本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcount", config, topologyBuilder.createTopology());
        }

    }
}
