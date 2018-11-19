package com.zqw.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String fileName;
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取文件名称
        FileSplit split = (FileSplit)context.getInputSplit();
        fileName = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split(" ");

        for (String field : fields) {
            k.set(field + "--" + fileName);
            context.write(k, v);
        }

    }
}
