package com.zqw.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text num = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split("\t");

//
//        List<String> fieldList = Arrays.asList(fields).stream()
//                .filter((x) -> !"".equals(x))
//                .filter((x) -> !" ".equals(x))
//                .collect(Collectors.toList());

//        List<String> fieldList = Arrays.asList(fields);

        flowBean.setSumFlow(Long.parseLong(fields[1]), Long.parseLong(fields[2]));

        num.set(fields[0]);
        context.write(flowBean, num);
    }
}
