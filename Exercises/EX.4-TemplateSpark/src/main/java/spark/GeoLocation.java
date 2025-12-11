package spark;

import java.io.Serializable;

public class GeoLocation implements Serializable {

    private String type;
    private double longitude;
    private double latitude;

    public GeoLocation() {
    }

    public GeoLocation(String type, double longitude, double latitude) {
        this.type = type;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    @Override
    public String toString() {
        return "GeoLocation{" +
                "type='" + type + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
}
