package hbase;

import data.Record;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  类描述：
 *      HBase数据库插入实现
 */
public class HBaseInsert {

    private final String RECORD_TABLENAME = "CorrectRecord";
    private final String FAMILY_NAME = "info";  //给出的用例中只有一个Record表，表中只有一个info列族

    public boolean insertRecordsToHBase(List<Record> allRecords){
        List<Put> allPuts = new ArrayList<>();
        for(Record record : allRecords) {
            //1.确定行键 （placeID##time##eid）
            String rowKey = record.getPlaceId() + "##" + record.getTime() + "##" + record.getEid();
            Put put = new Put(Bytes.toBytes(rowKey));
            //2.添加列
            put.addColumn(Bytes.toBytes(FAMILY_NAME) , Bytes.toBytes("address") , Bytes.toBytes(record.getAddress()));
            put.addColumn(Bytes.toBytes(FAMILY_NAME) , Bytes.toBytes("longitude") , Bytes.toBytes(record.getLongitude()+""));
            put.addColumn(Bytes.toBytes(FAMILY_NAME) , Bytes.toBytes("latitude") , Bytes.toBytes(record.getLatitude()+""));

            allPuts.add(put);
        }

        //3.执行数据库插入操作
        Table table = HBaseConf.getTableByName(RECORD_TABLENAME);
        try {
            table.put(allPuts);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
