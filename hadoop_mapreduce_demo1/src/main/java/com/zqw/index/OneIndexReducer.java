package com.zqw.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i=0;
        for (IntWritable value : values) {
            i += value.get();
        }

        v.set(i);
        context.write(key, v);
    }
}
