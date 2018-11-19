package com.zqw.wordcount;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1. 获取配置信息
        Configuration configuration = new Configuration();


        //ps 开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        //ps 设置map端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        //2. 获取jar包的位置
        job.setJarByClass(WordCountDriver.class);

        //3. 指定job要使用的mapper/Reducer的业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4. 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5. 指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6. 指定job输入输出原始文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //ps.设置读取输入文件切片的类（解决小文件问题）
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);  //4M
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);  //2M

        //8. 添加combiner
//        job.setCombinerClass(WordCountCombiner.class);

        //ps 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        //ps 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);



        //7. 将job中配置的相关参数，以及相关的java类所在的jar包交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
