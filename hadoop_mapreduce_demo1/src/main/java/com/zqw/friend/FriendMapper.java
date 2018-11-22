package com.zqw.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text, Text, Text>{
    Text k = new Text();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        //切出用户和好友
        String[] split1 = line.split(":");

        String user = split1[0];
        String[] friends = split1[1].split(",");

        for (String friend : friends) {
            k.set(friend);
            v.set(user);
            context.write(k, v);
        }
    }
}
