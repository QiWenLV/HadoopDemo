package com.zqw.uv;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class UVSpout extends BaseRichSpout{
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private String str;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/website.log"),"UTF-8"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            while((str = reader.readLine()) != null){

                collector.emit(new Values(str));

                Thread.sleep(500);
            }
        } catch (Exception e) {

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }
}
