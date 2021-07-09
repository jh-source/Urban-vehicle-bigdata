package edu.xidian.sselab.cloudcourse.domain;

public class GetTrack {
    String startTime;
    String endTime;
    String eid;

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public void setEid(String eid) {
        this.eid = eid;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getEid() {
        return eid;
    }
}
