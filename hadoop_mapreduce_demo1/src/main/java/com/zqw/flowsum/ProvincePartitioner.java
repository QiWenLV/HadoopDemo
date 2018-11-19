package com.zqw.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.zookeeper.txn.Txn;

/**
 * 自定义分区类
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //取手机号前3位
        String preNum = text.toString().substring(0, 3);

        int partition = 4;
        if("135".equals(preNum)){
            partition = 0;
        }else if("137".equals(preNum)){
            partition = 1;
        }else if("138".equals(preNum)){
            partition = 2;
        }else if("139".equals(preNum)){
            partition = 3;
        }

        return partition;
    }
}
