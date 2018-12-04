package com.zqw.demo2;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String>{


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //改变消息，加时间戳
        return new ProducerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.key(),
                System.currentTimeMillis() + "," + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

