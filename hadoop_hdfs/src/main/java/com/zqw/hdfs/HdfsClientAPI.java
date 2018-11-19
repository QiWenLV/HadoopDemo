package com.zqw.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


public class HdfsClientAPI {


    //获取文件系统
    @Test
    public void beans() throws IOException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 设置参数 
        configuration.set("fs.defaultFS", "hdfs://hadoop21:9000");

        // 3 获取文件系统
        FileSystem fs = FileSystem.get(configuration);

        // 4 打印文件系统
        System.out.println(fs.toString());
        fs.close();
    }
    

    //上传文件
    @Test
    public void putFileToHDFS() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        // 2 设置参数
        // 参数优先级： 1、客户端代码中设置的值  2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
        configuration.set("dfs.replication", "3");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),configuration, "wendy");

        //上传文件
        fs.copyFromLocalFile(new Path("f:/b/hello.txt"), new Path("hdfs://hadoop21:9000/user/wendy/hello.txt"));
        fs.close();
    }

    //文件下载
    @Test
    public void getFileFromHDFS() throws Exception{
        //文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        // 2 下载文件
        fs.copyToLocalFile(false, new Path("hdfs://hadoop21:9000/user/wendy/input/hello.txt"), new Path("f:/b/hellocopy.txt"), true);
        fs.close();
    }


    //创建文件夹
    @Test
    public void mkdirAtHDFS() throws Exception{

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        //2 创建目录
        fs.mkdirs(new Path("/user/wendy/output"));

        fs.close();
    }

    @Test
    public void deleteAtHDFS() throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        //删除路径，是否递归
        fs.delete(new Path("/user/wendy/output"), true);

        fs.close();
    }

    //修改文件名称
    @Test
    public void renameAtHDFS() throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        //删除路径，是否递归
        fs.rename(new Path("/user/wendy/input/hello.txt"), new Path("/user/wendy/input/hello1.txt"));

        fs.close();
    }

    //查看文件详细信息
    @Test
    public void readListFiles() throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        //文件路径，是否递归，返回一个迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            //下一个
            LocatedFileStatus status = listFiles.next();

            //输出详情
            System.out.println("文件名："+status.getPath().getName());
            //长度
            System.out.println("长度："+status.getLen());
            //权限
            System.out.println("权限："+status.getPermission());
            //组
            System.out.println("组："+status.getGroup());
            //获取块的位置信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("\n--------------------------------------------------\n");
        }

        fs.close();
    }

    //判断是文件还是文件夹
    @Test
    public void findAtHDFS() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop21:9000"),new Configuration(), "wendy");

        //获取查询路径下的文件状态信息
        FileStatus[] listStatus = fs.listStatus(new Path("/user/wendy/"));

        for(FileStatus status : listStatus){
            //如果是文件
            if (status.isFile()){
                System.out.println("f--" + status.getPath().getName());
            }else {
                System.out.println("d--" + status.getPath().getName());
            }
        }
        fs.close();

    }

}
