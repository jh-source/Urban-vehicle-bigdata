package edu.xidian.sselab.cloudcourse.domain;

import lombok.Data;
import lombok.ToString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

@Data
@ToString
public class VehicleCount {
    private Integer placeId;
    private String address;
    private Double longitude;
    private Double latitude;
    private Integer count;
    public VehicleCount() {
    }

    public VehicleCount(int placeId, String address, double longitude, double latitude,int count) {
        this.placeId = placeId;
        this.address = address;
        this.longitude = longitude;
        this.latitude = latitude;
        this.count=count;
    }

    public Integer getPlaceId() {
        return placeId;
    }

    public void setPlaceId(Integer placeId) {
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
    
    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
    public VehicleCount mapFrom(Result result) {
        String rowKey = Bytes.toString(result.getRow());
        setPlaceId(Integer.parseInt(rowKey));
        //解析所有的列信息
        List<Cell> cellList =  result.listCells();
        for (Cell cell : cellList) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            switch (qualifier) {
                case "address": setAddress(value); break;
                case "longitude": setLongitude(Double.parseDouble(value)); break;
                case "latitude": setLatitude(Double.parseDouble(value)); break;
                case "count":
                	int count = Bytes.toInt(CellUtil.cloneValue(cell));
                	setCount(count); break;
            }
        }
        return this;
    }
}
