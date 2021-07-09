package j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TopN {
    static String columnFamilyName = "info";
    static String inputTableName = "CorrectRecord";
    static String outputTableName = "MeetCount";

    public static void main(String[] args) {


        if (HbaseUtils.createTable(outputTableName, new String[]{columnFamilyName})) {
            JavaSparkContext sc = SC.getLocalSC("MeetCount");
            Configuration inputHbaseConf = HbaseConf.getConf();
            inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName);
            System.out.println("linked");
            Configuration outputHbaseConf = HbaseConf.getConf();
            JobConf jobConf = new JobConf(outputHbaseConf);
            jobConf.setOutputFormat(TableOutputFormat.class);
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);
            JavaPairRDD<ImmutableBytesWritable, Result> rdd1 = sc.newAPIHadoopRDD(inputHbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            JavaPairRDD<String, Tuple2<String, String>> p1 = rdd1.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, Tuple2<String, String>> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
                    String row = Bytes.toString(input._2.getRow());
                    String placeId = row.split("##")[0];
                    String time = row.split("##")[1];
                    String eid = row.split("##")[2];
                //    Long address = Bytes.toLong(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("")));
                    //   String latitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")));
                    //     String longitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")));
                    return new Tuple2<String, Tuple2<String, String>>(placeId, new Tuple2<>(time, eid));
                }
            });
            JavaPairRDD<String, Tuple2<String, String>> p2 = rdd1.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, Tuple2<String, String>> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
                    String row = Bytes.toString(input._2.getRow());
                    String placeId = row.split("##")[0];
                    String time = row.split("##")[1];
                    String eid = row.split("##")[2];

                  //  Long address = Bytes.toLong(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("")));
                    //   String latitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")));
                    //     String longitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")));
                    return new Tuple2<String, Tuple2<String, String>>(placeId, new Tuple2<>(time, eid));
                }
            });




            JavaPairRDD<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>> p4 = p1.join(p2).filter(new Function<Tuple2<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>> stringTuple2Tuple2) throws Exception {
                    Long time1 = Long.parseLong(stringTuple2Tuple2._2._1._1);
                    Long time2 = Long.parseLong(stringTuple2Tuple2._2._2._1);
                    Boolean a = !stringTuple2Tuple2._2._1._2.equals(stringTuple2Tuple2._2._2._2);
                    return ((Math.abs(time1 - time2) < 60) && a);
                }
            });

            JavaPairRDD<String, String> two_eid = p4.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>> stringTuple2Tuple2) throws Exception {
                    String str=stringTuple2Tuple2._2._1._2+"##"+stringTuple2Tuple2._2._2._2;
                    return new Tuple2<String, String>(str, stringTuple2Tuple2._2._2._2);
                }
            });


            //combinebykey()
            Function<String, Integer> start = new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return 1;
                }
            };
            Function2<Integer, String, Integer> merge = new Function2<Integer, String, Integer>() {
                @Override
                public Integer call(Integer integer, String s) throws Exception {
                    return integer + 1;
                }
            };

            Function2<Integer, Integer, Integer> combiner = new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            };
          two_eid.combineByKey(start, merge, combiner).mapToPair(new PairFunction<Tuple2<String, Integer>, ImmutableBytesWritable, Put>() {
                @Override
                public Tuple2<ImmutableBytesWritable,Put> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    String key = stringIntegerTuple2._1;
                    Integer count = stringIntegerTuple2._2;
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("count"), Bytes.toBytes(count));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                }
            }).saveAsHadoopDataset(jobConf);
         ///   pp.join()
           /*
            pp.join(two_eid).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, ImmutableBytesWritable, Put>() {
                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Tuple2<Integer, String>> stringTuple2Tuple2) throws Exception {
                    String key = stringTuple2Tuple2._1;
                    Integer count = stringTuple2Tuple2._2._1;
                    String eid2 = stringTuple2Tuple2._2._2;
                    Put put = new Put(Bytes.toBytes(key));
                    put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("eid2"), Bytes.toBytes(eid2));
                    put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("count"), Bytes.toBytes(count));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                }
            }).saveAsHadoopDataset(jobConf);
            */
            sc.stop();
        }
    }
}