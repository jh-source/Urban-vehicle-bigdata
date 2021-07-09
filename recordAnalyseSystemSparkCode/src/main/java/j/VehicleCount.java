package j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: 统计每个地点经过的车次，输出结果到vehicle_count表中，行健为placeId，列族为info，列名为address,latitude,longitude,count
 */
public class VehicleCount {
    static String columnFamilyName = "info";
    static String inputTableName = "CorrectRecord";
    static String outputTableName = "VehicleCount";

    public static void main(String[] args) {
        if (HbaseUtils.createTable(outputTableName, new String[]{columnFamilyName})) {
            JavaSparkContext sc = SC.getLocalSC("VehicleCount");
            Configuration inputHbaseConf = HbaseConf.getConf();
            inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName);

            Configuration outputHbaseConf = HbaseConf.getConf();
            JobConf jobConf = new JobConf(outputHbaseConf);
            jobConf.setOutputFormat(TableOutputFormat.class);
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);


            sc.newAPIHadoopRDD(inputHbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                    .mapToPair((Tuple2<ImmutableBytesWritable, Result> input) -> {
                                String row = Bytes.toString(input._2.getRow());
                                String placeId = row.split("##")[0];
                                String eid = row.split("##")[2];
                                String address = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address")));
                                String latitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")));
                                String longitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")));
                                return new Tuple2<Tuple4<String, String, String, String>, String>(new Tuple4<>(placeId, address, latitude, longitude), eid);
                            }
                    ).combineByKey((String eid) -> 1,
                    (Integer count, String eid) -> count + 1,
                    (Integer countA, Integer countB) -> countA + countB)
                    .mapToPair((Tuple2<Tuple4<String, String, String, String>, Integer> input) -> {
                        Tuple4<String, String, String, String> placeInfo = input._1;
                        Integer count = input._2;
                        Put put = new Put(Bytes.toBytes(placeInfo._1()));
                        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address"), Bytes.toBytes(placeInfo._2()));
                        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude"), Bytes.toBytes(placeInfo._3()));
                        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude"), Bytes.toBytes(placeInfo._4()));
                        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("count"), Bytes.toBytes(count));
                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    })
                    .saveAsHadoopDataset(jobConf);
            sc.stop();
        }
    }
}
