package edu.xidian.sselab.cloudcourse.domain;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CarMeet {
    String eid;
    String meetEid;
    int MeetCount;
    HashMap<String, Integer> meets = new HashMap<>();
    public CarMeet() {
    }

    public CarMeet(Result result) {
        this.eid = Bytes.toString(result.getRow());
        List<Cell> cellList = result.listCells();
        for (Cell cell : cellList) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
            meets.put(qualifier, value);
        }
    }

    public String getEid() {
        return eid;
    }

    public void setEid(String eid) {
        this.eid = eid;
    }

    public HashMap<String, Integer> getMeets() {
        return meets;
    }

    public void setMeets(HashMap<String, Integer> meets) {
        this.meets = meets;
    }

    public String getMeetEid() {
        return meetEid;
    }

    public void setMeetEid(String meetEid) {
        this.meetEid = meetEid;
    }

    public int getMeetCount() {
        return MeetCount;
    }

    public void setMeetCount(int meetCount) {
        MeetCount = meetCount;
    }
}
