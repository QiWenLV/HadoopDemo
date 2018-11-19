package com.zqw.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{

    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();


        //先区分两张表
        FileSplit split = (FileSplit)context.getInputSplit();

        String name = split.getPath().getName();

        if(name.startsWith("order")){   //订单表
            String[] fields = line.split("\t");
            v.setOrder_id(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("0");

            k.set(fields[1]);
        }else { //产品表
            String[] fields = line.split("\t");
            v.setPid(fields[0]);
            v.setPname(fields[1]);
            v.setOrder_id("");
            v.setAmount(0);
            v.setFlag("1");

            k.set(fields[0]);
        }

      context.write(k, v);
    }
}
