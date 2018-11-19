package com.zqw.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {

    public static void main(String[] args) {
        //1.测试压缩
//        compress("F:/b/input/web.log", "org.apache.hadoop.io.compress.BZip2Codec");
//        compress("F:/b/input/web.log", "org.apache.hadoop.io.compress.GzipCodec");
//        compress("F:/b/input/web.log", "org.apache.hadoop.io.compress.DefaultCodec,");

        //2.测试解压缩
        decompress("F:/b/input/web.log.bz2");
    }

    //测试解压缩
    private static void decompress(String fileName) {

        //0. 校验
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if(codec == null){
            System.out.println("不支持该解码器" + fileName);
            return;
        }
       
        try {
            //1. 获取输入流
            //压缩输入流
            CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));

            //2. 获取输出流
            FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

            //3. 流的对拷
            IOUtils.copyBytes(cis, fos, 1024*1024*5, false);
    
            //4. 关闭资源
            cis.close();
            fos.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //测试压缩
    private static void compress(String fileName, String method) {

        //1. 获取输入流
        try {
            FileInputStream fis = new FileInputStream(new File(fileName));

            //2. 获取输出流
            Class className = Class.forName(method);
            //通过反射工具类获取压缩方式对象
            CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(className, new Configuration());
            FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
            //将普通的输出流转化为压缩的输出流
            CompressionOutputStream cos = codec.createOutputStream(fos);

            //3. 流的对拷
            IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);    //false代表手动关流

            //4. 关闭资源
            fis.close();
            cos.close();
            fos.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
