package edu.xidian.sselab.cloudcourse.repository;

import edu.xidian.sselab.cloudcourse.domain.CarMeet;
import edu.xidian.sselab.cloudcourse.hbase.HbaseClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class MeetCountRespository {

    private static final String TABLE_NAME = "MeetCount";

    private static final String FAMILY_NAME = "info";

    private final HbaseClient hbaseClient;

    @Autowired
    public MeetCountRespository(HbaseClient hbaseClient) {
        this.hbaseClient = hbaseClient;
    }

    public List<CarMeet> findAllMeets(CarMeet carMeet){
        List<CarMeet> resultCarMeets = new ArrayList<>();
        hbaseClient.getConnection();
        Table table = hbaseClient.getTableByName(TABLE_NAME);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        if (StringUtils.isNotEmpty(carMeet.getEid())) {

            RowFilter rowFilter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(carMeet.getEid()));
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
                    CarMeet tempCarMeet = new CarMeet(result);
                    resultCarMeets.add(tempCarMeet);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询出错，返回一个空的列表!");
        }
        for(CarMeet carmeet:resultCarMeets){
            String pre_eid = carmeet.getEid();
            String[] str = pre_eid.split("##");
            String eidstr = str[1];
            String meetidstr = str[0];
            carmeet.setEid(eidstr);
            carmeet.setMeetEid(meetidstr);
            int cun = carmeet.getMeets().get("count");
            carmeet.setMeetCount(cun);
        }
        return resultCarMeets;
    }
}

