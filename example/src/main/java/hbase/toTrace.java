package hbase;

import data.Record;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class toTrace {

    private static HBaseConf hbaseClient;
    private static String FAMILY_NAME = "info";
    private static String RECORD_TABLENAME = "Trace";
    //rivate final String RECORD_TABLENAME ;
    //private final String FAMILY_NAME = "info";  //给出的用例中只有一个Record表，表中只有一个info列族

    public toTrace(HBaseConf hbaseClient) {
        this.hbaseClient = hbaseClient;
    }

    public static void getRecordsFromHbase() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.setInt("hbase.client.scanner.timeout.period",9000000);
        List<Record> resultRecords = new ArrayList<>();
        Connection conn = hbaseClient.getConnection();
        Table table = hbaseClient.getTableByName("CorrectRecord");
        Scan scan = new Scan();
        scan.addColumn("info".getBytes(), "latitude".getBytes());
        scan.addColumn("info".getBytes(), "longitude".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        int index = 1;
        List<Put> allPuts = new ArrayList<>();

        while (iterator.hasNext()) {
            Result result = iterator.next();
            System.out.println(index);
            int index1 = 0;
            String rowkey = "new";
            String[] val = new String[2];
            while (result.advance()) {
                Cell cell = result.current();
                rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                //String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                val[index1++] = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowkey + "--->" + qualifier + "--->" + val[index1 - 1]);
            }
            index += 1;
            String[] the_split = rowkey.split("##");
            String placeID = the_split[0];
            String time = the_split[1];
            String eid = the_split[2];
            System.out.println("placeid:" + placeID + "  time:" + time + "  eid:" + eid + "  latitude:" + val[0] + "  longitude:" + val[1]);

            //插入trace
            Put put = new Put(Bytes.toBytes(eid));
            //2.添加列
            put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("time"), Bytes.toBytes(time));
            put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("longitude"), Bytes.toBytes(val[0]));
            put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("latitude"), Bytes.toBytes(val[1]));
            allPuts.add(put);
            if(index%10000==0){
                Table table1 = hbaseClient.getTableByName(RECORD_TABLENAME);
                try {
                    table1.put(allPuts);
                } catch (IOException e) {
                    e.printStackTrace();
                    //return false;
                }
                allPuts.clear();
            }
        }
        //执行插入语句
        /*Table table1 = hbaseClient.getTableByName(RECORD_TABLENAME);
        try {
            table1.put(allPuts);
        } catch (IOException e) {
            e.printStackTrace();
            //return false;
        }*/
        //return true;

    }

    public static void testGet () {
        String hTableName = "Trace";
        Configuration conf = HBaseConfiguration.create();
        //IplaypiStudyConfig configuration = IplaypiStudyConfig.getInstance ();
        byte [] rowkey = "info".getBytes ();
        byte [] col01byte = "time".getBytes ();
        byte [] col02byte = "latitude".getBytes ();
        byte [] col03byte = "longitude".getBytes ();
        String rowToSearch = "00000985549233";
        try {
            // 构造查询请求，2 条数据，多个版本
            List<Get> getList = new ArrayList<>();
            Get get = new Get (Bytes.toBytes (rowToSearch));
            get.addColumn (rowkey, col01byte);
            get.addColumn (rowkey, col02byte);
            get.addColumn (rowkey, col03byte);
            get.setMaxVersions (20);
            getList.add (get);
            /*Get get2 = new Get (Bytes.toBytes ("row02"));
            get2.addColumn (cfbyte, col01byte);
            get2.addColumn (cfbyte, col02byte);
            getList.add (get2);*/
            // 发送请求，获取结果
            //HTable hTable = new HTable (configuration, hTableName);
            Table table = hbaseClient.getTableByName(hTableName);
            //HTable hTable = new HTable (conf,hTableName);
            Result [] resultArr = table.get (getList);
            /**
             * 以下有两种解析结果的方法
             * 1 - 通过 Result 类的 getRow () 和 getValue () 两个方法，只能获取最新版本
             * 2 - 通过 Result 类的 rawCells () 方法返回一个 Cell 数组，可以获取多个版本，如果使用 getColumnCells 可以指定列
             * 注意，高版本不再建议使用 KeyValue 的方式，注释中有说明
             */

            for (Result result : resultArr) {
                System.out.println("");
                System.out.println("--------");
                String rowStr = Bytes.toString (result.getRow ());
                System.out.println("====row:"+rowStr);
                //time列
                List<Cell> cellList = result.getColumnCells (rowkey, col01byte);
                // 1 个 cell 就是 1 个版本
                for (Cell cell : cellList) {
                    // 高版本不建议使用
                    //System.out.println("====name:[{}],getValue"+Bytes.toString (cell.getValue ()));
                    //getValueArray: 数据的 byte 数组
                    //getValueOffset:rowkey 在数组中的索引下标
                    //getValueLength:rowkey 的长度
                    //time列
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("time: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                }
                //latitude
                List<Cell> cellList1 = result.getColumnCells (rowkey, col02byte);
                for (Cell cell : cellList1) {
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("latitude: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                }
                //longitude
                List<Cell> cellList2 = result.getColumnCells (rowkey, col03byte);
                for (Cell cell : cellList2) {
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("longitude: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                }
            }
        }
        catch (IOException e) {
            System.out.println("!!!!error: " + e.getMessage ());
        }
    }


    public static void main(String[] args) throws IOException {

        getRecordsFromHbase();
        //testGet();
    }

}
