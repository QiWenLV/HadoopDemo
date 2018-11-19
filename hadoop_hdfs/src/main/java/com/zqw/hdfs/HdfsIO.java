package com.zqw.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用IO流来进行文件上传下载
 */
public class HdfsIO {

    //IO流上传文件
    @Test
    public void putFileToHDFS() throws Exception {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),configuration, "wendy");

        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File("f:/b/hello2.txt"));

        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/user/wendy/input/hello2.txt"));

        //4.流的拷贝
        IOUtils.copyBytes(fis, fos, configuration);
        //5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    //IO流下载文件
    @Test
    public  void  getFileFromHDFS()  throws IOException,  InterruptedException,
        URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),configuration, "wendy");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/wendy/input/hello2.txt"));
        // 3 获取输出流
        // 4 流对拷
        IOUtils.copyBytes(fis, System.out, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fis);
    }

    //定位读取
    @Test
    public void readFileSeek1() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"), configuration, "wendy");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/wendy/hadoop-2.7.2.tar.gz"));
        // 3 创建输出流(获取第一块)
        FileOutputStream fos  =  new  FileOutputStream(new File("f:/b/hadoop-2.7.2.tar.gz.part1"));
        // 4 流的拷贝
        byte[] buf = new byte[1024];
        //先读第一块(128M)
        for(int i=0; i<1024 * 12; i++){
            fis.read(buf);
            fos.write(buf);
        }

        //5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void readFileSeek2() throws Exception{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"), configuration, "wendy");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/user/wendy/hadoop-2.7.2.tar.gz"));
        // 3 定位输入数据位置
        fis.seek(1024*1024*128);
        // 4 创建输出流
        FileOutputStream  fos  =  new  FileOutputStream(new File("f:/b/hadoop-2.7.2.tar.gz.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

}
