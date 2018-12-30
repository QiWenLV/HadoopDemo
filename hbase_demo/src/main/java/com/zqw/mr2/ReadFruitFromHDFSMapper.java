package com.zqw.mr2;



import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");



        //设置rowKey
        byte[] rowKey = Bytes.toBytes(split[0]);
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowKey);
        Put put = new Put(rowKey);

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"), Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"), Bytes.toBytes(split[2]));

        context.write(immutableBytesWritable, put);

    }
}