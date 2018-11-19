package com.zqw.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
        //读缓存文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("F:/b/pd.txt"),"UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){

            String[] fields = line.split("\t");

            pdMap.put(fields[0], fields[1]);

        }

        //缓存数据到集合中
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        String pid = fields[1];
        //获取pid对应的产品名称
        String pdName = pdMap.get(pid);

        //拼接
        line = line + "\t" + pdName;

        k.set(line);
        context.write(k, NullWritable.get());
    }
}
