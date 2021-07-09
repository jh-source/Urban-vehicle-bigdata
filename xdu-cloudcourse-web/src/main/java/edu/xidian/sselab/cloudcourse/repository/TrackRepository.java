package edu.xidian.sselab.cloudcourse.repository;

import edu.xidian.sselab.cloudcourse.domain.GetTrack;
import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.hbase.HbaseClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Component
public class TrackRepository {

    private static final String TABLE_NAME = "TimeWithCars";

    private static final String FAMILY_NAME = "info";

    private final HbaseClient hbaseClient;

    @Autowired
    public TrackRepository(HbaseClient hbaseClient) {
        this.hbaseClient = hbaseClient;
    }

    public List<Record> findAllByRecord(GetTrack test) {
        List<Record> resultRecords = new ArrayList<>();
        hbaseClient.getConnection();
        Table table = hbaseClient.getTableByName(TABLE_NAME);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Scan scan = new Scan();
        if(StringUtils.isNotEmpty(test.getStartTime())){
            long startTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(test.getStartTime(), new ParsePosition(0))).getTime() / 1000;
            scan.setStartRow(Bytes.toBytes(String.format("%010d#%09d", startTime, 0)));
        }
        if(StringUtils.isNotEmpty(test.getEndTime())){
            long endTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(test.getEndTime(), new ParsePosition(0))).getTime() / 1000;
            scan.setStopRow(Bytes.toBytes(String.format("%010d#%-9d", endTime, 9).replace('-', '9')));
        }
        if (StringUtils.isNotEmpty(test.getEid())) {
            RowFilter rowFilter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(test.getEid() + "$"));
            filterList.addFilter(rowFilter);
        }
        if (filterList.getFilters().size() != 0) {
            scan.setFilter(filterList);
        }
        int number = 0;
        ResultScanner rs;
        try {
            if (table != null) {
                rs = table.getScanner(scan);
                System.out.println("has data, now deal a record");
                for (Result result : rs) {
                    number++;
                    if (number > 200) break;
                    System.out.println("enter a record, now parser it");
                    Record tempRecord = new Record().mapFrom2(result);
                    System.out.println(tempRecord.getEid());
                    resultRecords.add(tempRecord);
                }
            }
            System.out.println("resultsets done!");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询出错，返回一个空的列表!");
        }
        return resultRecords;
    }
}
