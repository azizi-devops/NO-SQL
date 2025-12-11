package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DataFrameAnalytics {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameAnalytics")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        String path = "src/main/resources/meteorite.json";

        Dataset<Row> meteorites = spark.read()
                .option("multiline", true)    // JSON is one big array
                .json(path);

        System.out.println("=== 4.2 a) Schema ===");
        meteorites.printSchema();

        averageMass(meteorites);
        fallClassification(meteorites);
        noCoordinates(meteorites);
        dataCleaning(meteorites);
        pointDistance(meteorites, spark);
        addDataPoints(meteorites, spark);

        spark.stop();
    }

    // 4.2 b) average mass of all meteorites
    public static void averageMass(Dataset<Row> df) {
        System.out.println("=== 4.2 b) Average mass ===");
        df.select(avg(col("mass").cast("double")).alias("avg_mass"))
                .show(false);
    }

    // 4.2 c) number of meteorites per "fall" category (Fell / Found)
    public static void fallClassification(Dataset<Row> df) {
        System.out.println("=== 4.2 c) Fall classification ===");
        df.groupBy("fall")
                .count()
                .orderBy(col("count").desc())
                .show(false);
    }

    // 4.2 d) impacts without geolocation (missing "geolocation" or coordinates)
    public static void noCoordinates(Dataset<Row> df) {
        System.out.println("=== 4.2 d) Missing coordinates ===");

        Dataset<Row> noCoords = df.filter(
                col("geolocation").isNull()
                        .or(col("geolocation.coordinates").isNull())
                        .or(size(col("geolocation.coordinates")).lt(2))
        );

        // show sorted by year (if present)
        noCoords.orderBy(asc("year"))
                .select("id", "name", "fall", "year")
                .show(false);
    }

    // 4.2 e) check inconsistencies between reclat/reclong and geolocation.coordinates
    public static void dataCleaning(Dataset<Row> df) {
        System.out.println("=== 4.2 e) Coordinate inconsistencies (ε = 0.1) ===");

        // reclat / reclong (strings in JSON) -> double
        Column lat1 = col("reclat").cast("double");
        Column lon1 = col("reclong").cast("double");

        // geolocation.coordinates is [longitude, latitude]
        Column lon2 = col("geolocation.coordinates").getItem(0).cast("double");
        Column lat2 = col("geolocation.coordinates").getItem(1).cast("double");

        double epsilon = 0.1;

        Dataset<Row> inconsistent = df.filter(
                lat1.isNotNull().and(lon1.isNotNull())
                        .and(lat2.isNotNull().and(lon2.isNotNull()))
                        .and(
                                abs(lat1.minus(lat2)).gt(epsilon)
                                        .or(abs(lon1.minus(lon2)).gt(epsilon))
                        )
        );

        inconsistent.select(
                        col("id"), col("name"),
                        lat1.alias("reclat"), lon1.alias("reclong"),
                        lat2.alias("geo_lat"), lon2.alias("geo_lon"))
                .show(false);
    }

    // 4.2 f) UDF for Haversine distance and 15 closest pairs of impacts
    public static void pointDistance(Dataset<Row> df, SparkSession spark) {
        System.out.println("=== 4.2 f) 15 closest pairs (Haversine) ===");

        // Prepare a compact dataset with id, name, lat, lon (use reclat/reclong)
        Dataset<Row> coords = df
                .filter(col("reclat").isNotNull().and(col("reclong").isNotNull()))
                .select(
                        col("id").alias("id"),
                        col("name").alias("name"),
                        col("reclat").cast("double").alias("lat"),
                        col("reclong").cast("double").alias("lon")
                );

        // Haversine UDF
        UserDefinedFunction haversine = udf(
                (Double lat1, Double lon1, Double lat2, Double lon2) -> {
                    if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
                        return null;
                    }
                    double R = 6371.0; // earth radius in km
                    double dLat = Math.toRadians(lat2 - lat1);
                    double dLon = Math.toRadians(lon2 - lon1);
                    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                            + Math.cos(Math.toRadians(lat1))
                            * Math.cos(Math.toRadians(lat2))
                            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
                    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                    return R * c;
                },
                DataTypes.DoubleType
        );

        spark.udf().register("haversine", haversine);

        Dataset<Row> pairs = coords.alias("a")
                .crossJoin(coords.alias("b"))
                .filter(col("a.id").lt(col("b.id")))          // avoid duplicates & self-pairs
                .withColumn("distance",
                        callUDF("haversine",
                                col("a.lat"), col("a.lon"),
                                col("b.lat"), col("b.lon")))
                .orderBy(asc("distance"))
                .limit(15);

        pairs.select(
                col("a.id").alias("id1"),
                col("a.name").alias("name1"),
                col("b.id").alias("id2"),
                col("b.name").alias("name2"),
                col("distance")
        ).show(false);
    }

    // 4.2 g) create Dataset<GeoLocation>, add 3 custom points, and union
    public static void addDataPoints(Dataset<Row> df, SparkSession spark) {
        System.out.println("=== 4.2 g) Union with custom GeoLocation points ===");

        Encoder<GeoLocation> geoEncoder = Encoders.bean(GeoLocation.class);

        // Extract GeoLocation from JSON:
        // type  -> geolocation.type
        // lon   -> geolocation.coordinates[0]
        // lat   -> geolocation.coordinates[1]
        Column lon = col("geolocation.coordinates").getItem(0).cast("double");
        Column lat = col("geolocation.coordinates").getItem(1).cast("double");

        Dataset<GeoLocation> original = df
                .filter(col("geolocation").isNotNull()
                        .and(lon.isNotNull())
                        .and(lat.isNotNull()))
                .select(
                        col("geolocation.type").alias("type"),
                        lon.alias("longitude"),
                        lat.alias("latitude")
                )
                .as(geoEncoder);

        // three additional arbitrary locations
        GeoLocation g1 = new GeoLocation("Point", 8.766, 50.809);   // e.g. Marburg
        GeoLocation g2 = new GeoLocation("Point", 13.405, 52.520);  // Berlin
        GeoLocation g3 = new GeoLocation("Point", -0.127, 51.507);  // London

        List<GeoLocation> extraList = Arrays.asList(g1, g2, g3);

        Dataset<GeoLocation> extra = spark
                .createDataset(extraList, geoEncoder)
                .select("type", "longitude", "latitude")
                .as(geoEncoder);

        Dataset<GeoLocation> allLocations = original.union(extra);

        allLocations.show(false);
    }
}
