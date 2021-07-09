package j;

import akka.japi.Util;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        JavaSparkContext sc = SC.getLocalSC("test");
        List<String>  dataList=new ArrayList<String>();
        dataList.add("11,12,13,14,15");
        dataList.add("aa,dd,bb,cc,dd");
        JavaRDD<String> list=sc.parallelize(dataList);
        JavaRDD<String[]> map=list.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        });

    }

}