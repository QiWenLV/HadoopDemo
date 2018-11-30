package com.zqw.hive_youtube.etl;

import com.zqw.hive_youtube.utils.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VideoETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String etlString = ETLUtil.getETLString(line);
        if(etlString != null){
            v.set(etlString);
            context.write(NullWritable.get(), v);
        }

    }
}
