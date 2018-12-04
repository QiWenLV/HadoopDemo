package com.zqw.demo3;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class App {

    public static void main(String[] args) {
        String fromTopic = "test2";
        String toTopic = "test3";

        //设置参数
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop21:9092,hadoop22:9092,hadoop23:9092");

        //实例化StreamConfig
        StreamsConfig config = new StreamsConfig(props);
        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        //指定数据来源，处理，去向
        builder.addSource("SOURCE", fromTopic)
                .addProcessor("PROESSOR", ()->{
                    return new LogProcessor();
                }, "SOURCE")
                .addSink("SINK", toTopic, "PROESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
