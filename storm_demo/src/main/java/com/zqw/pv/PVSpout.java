package com.zqw.pv;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class PVSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private String str = null;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //读取文件
        this.collector = collector;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream("f:/b/input/websile.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        //发送文件
        try {
            while ((str = reader.readLine()) != null){
                collector.emit(new Values(str));

                //延时
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //发送字段
        declarer.declare(new Fields("log"));
    }
    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }



    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
