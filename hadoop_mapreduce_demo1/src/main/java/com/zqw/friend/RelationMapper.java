package com.zqw.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");

        String friend = split[0];
        String[] users = split[1].split(",");


        for (int i = 0; i < users.length - 1; i++) {
            for (int j = i+1; j < users.length; j++) {
                k.set(users[i] + "-" + users[j]);
                v.set(friend);
                context.write(k, v);
            }
        }
    }
}
