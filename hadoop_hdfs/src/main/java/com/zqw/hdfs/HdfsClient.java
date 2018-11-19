package com.zqw.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;

public class HdfsClient {

    public static void main(String[] args) throws Exception {

        //1. 获取文件系统
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop21:9000");
//        FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"), configuration, "wendy");

        //2. 上传文件,
        fs.copyFromLocalFile(new Path("f:/b/hello.txt"), new Path("/user/wendy/input"));

        //3. 关闭资源
        fs.close();

        System.out.println("执行完毕");
    }
}
