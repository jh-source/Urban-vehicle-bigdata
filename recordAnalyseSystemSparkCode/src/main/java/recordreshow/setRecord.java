package recordreshow;

import j.HbaseConf;
import j.SC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Iterator;

public class setRecord {
    static String columnFamilyName = "info";
    static String inputTableName = "Record";
    static String outputTableName = "recordReshow";
    static Iterator<Tuple4<String,String,String,String>>my;
    static String find;
    public static Iterator<Tuple4<String, String, String, String>> getMy() {
        return my;
    }

    public static void setFind(String find) {
        setRecord.find = find;
    }
    public static void init()
    {
        Iterator<Tuple4<String, String, String, String>> my_want = null;
        JavaSparkContext sc = SC.getLocalSC("recordReshow");
        Configuration inputHbaseConf = HbaseConf.getConf();
        inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName);
        System.out.println("linked");
        Configuration outputHbaseConf = HbaseConf.getConf();
        JobConf jobConf = new JobConf(outputHbaseConf);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);
        JavaPairRDD<ImmutableBytesWritable, Result> rdd1 = sc.newAPIHadoopRDD(inputHbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaPairRDD<String, Tuple4<String, String, String, String>> p1 = rdd1.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple2<String, Tuple4<String, String, String, String>> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {

                String row = Bytes.toString(immutableBytesWritableResultTuple2._2.getRow());
                String time = row.split("##")[1];
                String eid = row.split("##")[2];
                String address = Bytes.toString(immutableBytesWritableResultTuple2._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address")));
                String latitude = Bytes.toString(immutableBytesWritableResultTuple2._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")));
                String longitude = Bytes.toString(immutableBytesWritableResultTuple2._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")));
                return new Tuple2<String, Tuple4<String, String, String, String>>(eid, new Tuple4<>(time, longitude, latitude, address));
            }
        });

        JavaPairRDD<String, Iterable<Tuple4<String, String, String, String>>> p2 = p1.groupByKey();
        p2.foreach(new VoidFunction<Tuple2<String, Iterable<Tuple4<String, String, String, String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple4<String, String, String, String>>> stringIterableTuple2) throws Exception {
                //  System.out.println("Group_id: =" + stringIterableTuple2._1());
                String key = stringIterableTuple2._1();
                if (key.equals(find)) {
                    Iterator<Tuple4<String, String, String, String>> value = (Iterator<Tuple4<String, String, String, String>>) stringIterableTuple2._2().iterator();
                    my=value;
                   /* while (value.hasNext()) {
                        Tuple4<String, String, String, String> temp = value.next();
                        System.out.println("time:" + temp._1() + "longtitude ;" + temp._2() + "latitude:" + temp._3() + "address:" + temp._4());
                    }
                    */
                 //   System.out.println("*******************************");

                }

            }
        });


    }
}
