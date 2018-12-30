package com.zqw.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PVSumBolt implements IRichBolt {

    private Map<Long, Integer> NumMap = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        //累加求和逻辑

        //获取数据
        Long threadId = input.getLongByField("threadId");
        Integer pvNum = input.getIntegerByField("pvNum");

        NumMap.put(threadId, pvNum);

        //求和
        Iterator<Integer> iterator = NumMap.values().iterator();

        int sumnum = 0;

        while (iterator.hasNext()) {
            sumnum += iterator.next();
        }

        //打印
        System.err.println("sumnum: " + sumnum);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
