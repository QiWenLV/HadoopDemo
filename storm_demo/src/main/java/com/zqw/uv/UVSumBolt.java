package com.zqw.uv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class UVSumBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<>();


    @Override

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {

        String ip = input.getStringByField("ip");
        Integer num = input.getIntegerByField("num");

        if (map.containsKey(ip)) {
            //包含
            Integer count = map.get(ip);
            map.put(ip, count + num);
        } else {
            map.put(ip, num);
        }

        //打印
        System.out.println("ip:" + ip + " uv:" + map.get(ip));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
