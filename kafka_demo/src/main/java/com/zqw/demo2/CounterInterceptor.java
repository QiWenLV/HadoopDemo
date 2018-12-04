package com.zqw.demo2;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private long successCount = 0;
    private long errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 查看是否异常
        if(e == null){
            successCount++;
        }else{
            errorCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功的个数：" + successCount);
        System.out.println("失败的个数：" + errorCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
