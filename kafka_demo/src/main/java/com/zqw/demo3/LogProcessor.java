package com.zqw.demo3;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        //处理数据
        String inputOri = new String(value);

        if(inputOri.contains(">>>")){
            inputOri = inputOri.split(">>>")[1];
        }
        context.forward(key, inputOri.getBytes());

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
