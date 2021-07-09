package edu.xidian.sselab.cloudcourse.hbase;

import edu.xidian.sselab.cloudcourse.domain.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class getTrace {
    private static HbaseClient hbaseClient;

    public static List<Record> testGet (String rowToSearch,String stime,String etime) {
        String hTableName = "Trace";
        Configuration conf = HBaseConfiguration.create();

        byte [] rowkey = "info".getBytes ();
        byte [] col01byte = "time".getBytes ();
        byte [] col02byte = "latitude".getBytes ();
        byte [] col03byte = "longitude".getBytes ();
        List<Record> rd = new ArrayList<Record>();
        //String rowToSearch = "00001791319211";
        try {
            // 构造查询请求，2 条数据，多个版本
            List<Get> getList = new ArrayList<>();
            Get get = new Get (Bytes.toBytes (rowToSearch));
            get.addColumn (rowkey, col01byte);
            get.addColumn (rowkey, col02byte);
            get.addColumn (rowkey, col03byte);
            get.setMaxVersions (20);
            getList.add (get);

            Table table = hbaseClient.getTableByName(hTableName);
            Result[] resultArr = table.get (getList);
            //List<Record> rd = new ArrayList<Record>();
            Record temp[] = new Record[20];
            for (Result result : resultArr) {
                System.out.println("");
                System.out.println("--------");
                String rowStr = Bytes.toString (result.getRow ());
                System.out.println("====row:"+rowStr);
                for(int i=0;i<20;i++){
                    temp[i] = new Record();
                }
                //time列
                List<Cell> cellList = result.getColumnCells (rowkey, col01byte);
                // 1 个 cell 就是 1 个版本
                int index=0;
                for (Cell cell : cellList) {
                    //time列
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("time: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                    long vallong = Long.parseLong(valStr);
                    //temp[index] = new Record();
                    temp[index].setTime(vallong);
                    index++;
                }
                //latitude
                index=0;
                List<Cell> cellList1 = result.getColumnCells (rowkey, col02byte);
                for (Cell cell : cellList1) {
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("latitude: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                    double valdouble = Double.parseDouble(valStr);

                    temp[index].setLatitude(valdouble);
                    index++;
                }
                //longitude
                index=0;
                List<Cell> cellList2 = result.getColumnCells (rowkey, col03byte);
                for (Cell cell : cellList2) {
                    String valStr = Bytes.toString (cell.getValueArray (), cell.getValueOffset (), cell.getValueLength ());
                    System.out.print("longitude: "+valStr);
                    System.out.println("  timestamp:"+cell.getTimestamp ());
                    double valdouble = Double.parseDouble(valStr);

                    temp[index].setLongitude(valdouble);
                    index++;
                }
                /*if(stime.length()!=0&&etime.length()!=0){
                    for (int j = 0; j < index; j++) {

                        if (temp[j].getTime() >= Long.parseLong(stime) && temp[j].getTime() <= Long.parseLong(etime))
                            rd.add(temp[j]);
                    }
                }
                else if(stime.length()==0&&etime.length()==0){
                    for (int j = 0; j < index; j++) {
                        rd.add(temp[j]);
                    }
                }*/
                for (int j = 0; j < index; j++) {
                    rd.add(temp[j]);
                }
            }
        }
        catch (IOException e) {
            System.out.println("!!!!error: " + e.getMessage ());
        }

        return rd;
    }
    /*public static void main(String[] args) throws IOException {
        testGet();
    }*/
}
