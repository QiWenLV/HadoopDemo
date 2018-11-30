package com.zqw.hive_youtube.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class VideoETLRunner implements Tool{

    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            int resultCode = ToolRunner.run(new VideoETLRunner(), args);
            if(resultCode == 0){
                System.out.println("Success!");
            }else{
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        conf.set("inpath", args[0]);
        conf.set("outpath", args[1]);

        Job job = Job.getInstance(conf);

        job.setJarByClass(VideoETLRunner.class);
        job.setMapperClass(VideoETLMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        //设置文件输入输出类型
        initJobInputPath(job);
        initJobOutputPath(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    private void initJobOutputPath(Job job) throws IOException {
        //通过job获取Configuration
        Configuration conf = job.getConfiguration();

        //在Configuration中获取输出路径
        Path outPath = new Path(conf.get("outpath"));
        //获取文件系统
        FileSystem fs = FileSystem.get(conf);

        //目录已经存在
        if(fs.exists(outPath)){
            fs.delete(outPath, true);
        }


        FileOutputFormat.setOutputPath(job, outPath);

    }

    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();


        //获取文件系统
        FileSystem fs = FileSystem.get(conf);
        //获取文件输入路径
        Path inPath = new Path(conf.get("inpath"));

        //文件夹已经存在
        if(fs.exists(inPath)){
            FileInputFormat.addInputPath(job, inPath);
        }else{
            throw new RuntimeException("HDFS 中该文件目录不存在：" + inPath.toString());
        }
    }
}
