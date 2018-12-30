package com.zqw.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

/**
 * Spout: 读取外部文件
 */
public class ReadFileSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private  BufferedReader reader;

    /**
     * 初始化方法，类似于构造器
     * 一般用来打开数据库连接，打开网络连接
     * @param map   //配置文件
     * @param topologyContext   //上下文对象
     * @param spoutOutputCollector  //数据输出的收集器，spout类将数据发给Collector，Collector发给storm框架
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            //读取文件
            File file1 = new File("/opt/module/datas/wordcount.txt");
//            File file1 = new File("F:\\b\\input\\新建文本文档.txt");
            reader = new BufferedReader(new FileReader(file1));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.collector = spoutOutputCollector;
    }

    /**
     * 获取下一个数据
     */
    @Override
    public void nextTuple() {

        String line = null;
        try {
            line = reader.readLine();
            if (line != null) {
                //提交数据
                collector.emit(Arrays.asList(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 为发送数据设置字段名
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("juzi"));
    }
}
