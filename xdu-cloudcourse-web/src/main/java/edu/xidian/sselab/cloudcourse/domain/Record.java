package edu.xidian.sselab.cloudcourse.domain;

import lombok.Data;
import lombok.ToString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.List;

@Data
@ToString
public class Record {

    private String eid;

    private Long time;

    private Long placeId;

    private String address;

    private Double longitude;

    private Double latitude;

    private String formatTime;

    public Record mapFrom(Result result) {
        // 1.分解行键
        String[] rowKey = Bytes.toString(result.getRow()).split("##");
        setPlaceId(Long.parseLong(rowKey[0]));
        setTime(Long.parseLong(rowKey[1]));
        setEid(rowKey[2]);
        // 2.解析所有的列信息
        List<Cell> cellList = result.listCells();
        for (Cell cell : cellList) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "address":
                    setAddress(value);
                    break;
                case "longitude":
                    setLongitude(Double.parseDouble(value));
                    break;
                case "latitude":
                    setLatitude(Double.parseDouble(value));
                    break;
            }
        }

        return this;
    }

    public Record mapFrom2(Result result) {
        // 1.分解行键
        String[] rowKey = Bytes.toString(result.getRow()).split("#");
//        setPlaceId(Long.parseLong(rowKey[0]));
        setTime(Long.parseLong(rowKey[0]));
        setEid(rowKey[1]);
        // 2.解析所有的列信息
        List<Cell> cellList = result.listCells();
        for (Cell cell : cellList) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "PlaceId":
                    setPlaceId(Bytes.toLong(CellUtil.cloneValue(cell)));
                    break;
                case "address":
                    setAddress(value);
                    break;
                case "longitude":
                    setLongitude(Double.parseDouble(value));
                    break;
                case "latitude":
                    setLatitude(Double.parseDouble(value));
                    break;
            }
        }
        return this;
    }

    public Record mapFrom3(Result result) {
        // 1.分解行键
        String rowKey = Bytes.toString(result.getRow());
        //setPlaceId(Long.parseLong(rowKey[0]));
        //setTime(Long.parseLong(rowKey[1]));
        setEid(rowKey);
        // 2.解析所有的列信息
        List<Cell> cellList = result.listCells();
        for (Cell cell : cellList) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "time":
                    setAddress(value);
                    break;
                case "longitude":
                    setLongitude(Double.parseDouble(value));
                    break;
                case "latitude":
                    setLatitude(Double.parseDouble(value));
                    break;
            }
        }

        return this;
    }

    public String getEid() {
        return eid;
    }

    public void setEid(String eid) {
        this.eid = eid;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getPlaceId() {
        return placeId;
    }

    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public String getFormatTime() {
        return formatTime;
    }

    public void setFormatTime(String formatTime) {
        this.formatTime = formatTime;
    }

    public String getTimeWithFormat() {
        // 这里时间戳以毫秒为单位, 以秒为单位要除以1000
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.time * 1000);
    }
}
