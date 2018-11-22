package com.zqw.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        context.write(key, new Text(sb.toString()));
    }
}
