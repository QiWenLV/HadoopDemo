package com.zqw.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

/**
 * Bolt: 切割一行数据
 */
public class SplitBolt extends BaseRichBolt{

    private OutputCollector collector;
    /**
     * 初始化方法
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * 执行业务逻辑
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        //获取上游字段
        String juzi = tuple.getStringByField("juzi");
        //对句子进行切割
        String[] words = juzi.split(" ");
        //发送数据
        for (String word : words) {
            //需要发送单词及单词出现的次数，
            collector.emit(Arrays.asList(word, 1));
        }
    }

    /**
     * 设置字段名
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "num"));
    }
}
