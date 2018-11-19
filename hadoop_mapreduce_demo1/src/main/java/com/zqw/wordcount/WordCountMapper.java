package com.zqw.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入的key       LongWritable    行号
 * 输入的value     Text            一行内容
 * 输出的key       Text            单词
 * 输出的value     IntWritable     单词的个数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     *
     * @param key       行号
     * @param value     一行单词
     * @param context   上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将Text转换为String
        String line = value.toString();
        //切割成单词
        String[] words = line.split(" ");

        //循环写入到下一个阶段
        for (String word : words) {
            k.set(word);
            //参数是下一个阶段的是Key和Value
            context.write(k, v);
        }

    }
}
