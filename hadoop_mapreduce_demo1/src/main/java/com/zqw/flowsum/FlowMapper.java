package com.zqw.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text k = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split(" ");



        List<String> fieldList = Arrays.asList(fields).stream()
                .filter((x) -> !"".equals(x))
                .filter((x) -> !" ".equals(x))
                .collect(Collectors.toList());


        if(fieldList.size() < 3){
            return;
        }
        k.set(fieldList.get(1));

        Long upFlow = Long.parseLong(fieldList.get(fieldList.size() - 3));
        Long downFlow = Long.parseLong(fieldList.get(fieldList.size() - 2));
        flowBean.setSumFlow(upFlow, downFlow);

        context.write(k, flowBean);
    }
}
