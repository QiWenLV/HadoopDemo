package com.zqw.hive_demo1;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Lower extends UDF {

    //方法名evaluate，支持重载
    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }
        return s.toString().toLowerCase();
    }
}
