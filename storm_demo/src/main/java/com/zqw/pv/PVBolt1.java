package com.zqw.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PVBolt1 implements IRichBolt {

    private OutputCollector collector;
    private int pvNum = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //业务处理
        String log = input.getStringByField("log");

        //截取
        String[] split = log.split("\t");
        String session = split[1];

        if (session != null) {

            //局部累加
            pvNum ++;

            //输出
            collector.emit(new Values(Thread.currentThread().getId(), pvNum));
        }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明字段
        declarer.declare(new Fields("threadId", "pvNum"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
