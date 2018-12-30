package com.zqw.pv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class PVMain {

    public static void main(String[] args) {

        //创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("PVSpout", new PVSpout(), 1);
        builder.setBolt("PVBolt1", new PVBolt1(), 3).shuffleGrouping("PVSpout");
        builder.setBolt("PVSumBolt", new PVSumBolt(), 1).shuffleGrouping("PVBolt1");

        //配置信息
        Config conf = new Config();

        //提交
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("pvtopology", conf, builder.createTopology());
    }
}
