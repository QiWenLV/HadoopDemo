package com.zqw.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Bolt: 负责统计每个单词出现的次数
 */
public class WordCountBolt extends BaseRichBolt{

    private Map<String, Integer> wordCountMap = new HashMap<>();
    private int count;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //最后一个bolt了， 不需要收集器
        count = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        //获取数据（单词，次数）
        String word = tuple.getStringByField("word");
        int num = tuple.getIntegerByField("num");

        //定义Map
        if (wordCountMap.containsKey(word)) {
            wordCountMap.put(word, wordCountMap.get(word) + num);
        } else {
            wordCountMap.put(word, num);
        }

        //打印到控制台
        System.out.println(count + ": " +  word+ "-----" + wordCountMap.get(word));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //不需要向下传递了，所以不需要重写这个方法
        count++;
    }
}
