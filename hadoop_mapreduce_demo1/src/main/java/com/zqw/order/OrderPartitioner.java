package com.zqw.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable>{
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {

        //i为分区数
        return (orderBean.getOrder_id() & Integer.MAX_VALUE) % i;

    }
}
