package com.zqw.hive_youtube.utils;
//已阅
//格式写的不错;

public class ETLUtil {

    //数据清洗
    //Ddn4MGaS3N4	rpoland	648	Music	198	2417724	4.89	18998	4696	JsD6uEZsIsU	dt1fB62cGbo	3K5qLF9qzZo	l_ZNi5ODXKQ	0MC8bpRXY98	Cvar4ZsqsEo	4pbSvsIVTwQ	8OSq-8HABIA	0Ctm0xkjOC8	6osCM7nuVTc	Sejwla4jcYY	ksv-HK9btVE	qMndTAWFP1E	IOmAm-KKBU0	VFbMjAzpzG0	oJFtu6GkvXQ	2aIYAJG3gZ4	MKbOsAoUegI	podDDIhITgk	FWjZEO3yqI4
    public static String getETLString(String ori){

        String[] splitsArray = ori.split("\t");

        StringBuilder sb = new StringBuilder();

        if(splitsArray.length < 9){
            return null;
        }
        //先处理category的空格问题
        splitsArray[3] = splitsArray[3].replaceAll(" ", "");

        for(int i=0; i<splitsArray.length; i++){
            sb.append(splitsArray[i]);
            if(i < 9){
                //相关id之前的数据
                if(i != splitsArray.length - 1){
                    sb.append("\t");
                }
            }else {
                //相关id之后的数据
                if(i != splitsArray.length - 1){
                    sb.append("$");
                }
            }
        }


        return sb.toString();
    }

}
