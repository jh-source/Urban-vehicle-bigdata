package edu.xidian.sselab.cloudcourse.repository;

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
public class FliterRepository {

    private static final String TABLE_NAME = "WrongRecord";

    private static final String FAMILY_NAME = "info";

    private final HbaseClient hbaseClient;

    @Autowired
    public FliterRepository(HbaseClient hbaseClient) {
        this.hbaseClient = hbaseClient;
    }

    public List<Record> findAllByRecord(Record record){
        List<Record> resultRecords = new ArrayList<>();
        hbaseClient.getConnection();
        Table table = hbaseClient.getTableByName(TABLE_NAME);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        if (record.getPlaceId() != null) {
            RowFilter rowFilter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("^" + record.getPlaceId() + "##"));
            filterList.addFilter(rowFilter);
        }
        if (StringUtils.isNotEmpty(record.getFormatTime())) {
            long timestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(record.getFormatTime(), new ParsePosition(0)).getTime()) / 1000;
            RowFilter rowFilter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("##" + timestamp + "##"));
            filterList.addFilter(rowFilter);
        }
        if (StringUtils.isNotEmpty(record.getEid())) {
            RowFilter rowFilter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator("##" + record.getEid() + "$"));
            filterList.addFilter(rowFilter);
        }

        Scan scan = new Scan();
        if (filterList.getFilters().size() != 0) {
            scan.setFilter(filterList);
        }
        ResultScanner rs;
        try {
            if (table != null) {
                rs = table.getScanner(scan);
                for (Result result : rs) {
                    Record tempRecord = new Record().mapFrom(result);
                    resultRecords.add(tempRecord);
                    if(resultRecords.size() >= 200)
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询出错，返回一个空的列表!");
        }
        return resultRecords;
    }

}
