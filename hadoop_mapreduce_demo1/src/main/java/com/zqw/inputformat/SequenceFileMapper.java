package com.zqw.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //一次性获取所有的文件路径和名称

        //获取切片信息
        FileSplit split = (FileSplit)context.getInputSplit();

        Path path = split.getPath();

        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //value就是文件里的信息，只需要加上一个key就可以了。
        context.write(k, value);

    }
}
