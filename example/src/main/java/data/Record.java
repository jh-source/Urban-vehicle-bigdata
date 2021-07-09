package data;

/**
 * Author: cwz
 * Time: 2017/9/19
 * Description:
 */
public class Record {
    private String eid;
    private long time;
    private int placeId;
    private String address;
    private double longitude;
    private double latitude;


    public Record() {
    }

    public Record(String eid, long time, int placeId, String address, double longitude, double latitude) {
        this.eid = eid;
        this.time = time;
        this.placeId = placeId;
        this.address = address;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public String getEid() {
        return eid;
    }

    public void setEid(String eid) {
        this.eid = eid;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getPlaceId() {
        return placeId;
    }

    public void setPlaceId(int placeId) {
        this.placeId = placeId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
}
