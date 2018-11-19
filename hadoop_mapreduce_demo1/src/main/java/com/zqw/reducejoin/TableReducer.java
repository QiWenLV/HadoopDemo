package com.zqw.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        try {
            for (TableBean value : values) {
                if("1".equals(value.getFlag())){    //产品表

                    BeanUtils.copyProperties(pdBean, value);
                }else {     //订单表

                    TableBean tableBean = new TableBean();

                    BeanUtils.copyProperties(tableBean, value);
                    orderBeans.add(tableBean);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        //拼接
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());

            context.write(orderBean, NullWritable.get());
        }
    }
}
