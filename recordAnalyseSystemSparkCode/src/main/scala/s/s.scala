package s

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object s {
  val columnFamilyName = "info"
  val inputTableName = "Record"
  val outputTableName = "VehicleCount"

  if (HbaseUtils.createTable(outputTableName, Array(columnFamilyName))) {
    val sc = SC.getLocalSC("VehicleCount")
    val inputHbaseConf = HbaseConf.getConf()
    inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName)

    val outputHbaseConf = HbaseConf.getConf()
    val jobConf = new JobConf(outputHbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

    sc.newAPIHadoopRDD(inputHbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map { case (_, input) => {
        val row: String = Bytes.toString(input.getRow)
        val placeId: String = row.split("##")(0)

        val eid: String = row.split("##")(2)
        val address: String = Bytes.toString(input.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address")))
        val latitude: String = Bytes.toString(input.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")))
        val longitude: String = Bytes.toString(input.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")))
        ((placeId, address, latitude, longitude), eid)
      }}
      .combineByKey(x => 1,
        (x: Int, y: String) => x + 1,
        (x: Int, y: Int) => x + y)
      .map { case (placeInfo, count) => {
        val put = new Put(Bytes.toBytes(placeInfo._1))
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address"), Bytes.toBytes(placeInfo._2))
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude"), Bytes.toBytes(placeInfo._3))
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude"), Bytes.toBytes(placeInfo._4))
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("count"), Bytes.toBytes(count))
        (new ImmutableBytesWritable, put)
      }}
      .saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
