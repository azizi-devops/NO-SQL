package project2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.sql.utils.Adapter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

public class GBifPreProcessing {

    // Mongo
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "biodiv";
    private static final String MONGO_COLLECTION = "gbif";

    /** Spark + Sedona + Mongo connector */
    public static SparkSession getSedonaMongoSession() {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("GBIF-PreProcessing")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

                // Mongo Spark Connector 10.x
                .config("spark.mongodb.read.connection.uri", MONGO_URI)
                .config("spark.mongodb.write.connection.uri", MONGO_URI)
                .config("spark.mongodb.read.database", MONGO_DB)
                .config("spark.mongodb.read.collection", MONGO_COLLECTION)
                .config("spark.mongodb.write.database", MONGO_DB)
                .config("spark.mongodb.write.collection", MONGO_COLLECTION)

                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        // Register Sedona SQL functions (ST_Contains, ST_GeomFromGeoJSON, ST_AsGeoJSON, ...)
        SedonaContext.create(spark);

        return spark;
    }

    /** Read FeatureCollection GeoJSON: { "type":"FeatureCollection", "features":[ ... ] } */
    public static Dataset<Row> readGbif(SparkSession spark, String geojsonPath) {

        Path p = Paths.get(geojsonPath);
        if (!Files.exists(p)) {
            throw new RuntimeException("GeoJSON file NOT FOUND:\n" + p.toAbsolutePath());
        }

        Dataset<Row> raw = spark.read()
                .option("multiline", "true")          // IMPORTANT for GeoJSON FeatureCollection
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(p.toAbsolutePath().toString());

        System.out.println("RAW SCHEMA:");
        raw.printSchema();

        // If parsing failed, you will only see _corrupt_record
        if (raw.columns().length == 1 && raw.columns()[0].equals("_corrupt_record")) {
            raw.select(col("_corrupt_record")).show(5, false);
            throw new RuntimeException(
                    "GeoJSON was NOT parsed (only _corrupt_record exists).\n" +
                            "Your file is either NOT a FeatureCollection, or it needs multiline=true, or it is not valid JSON."
            );
        }

        // explode features array -> feature struct
        Dataset<Row> features = raw.select(explode(col("features")).alias("f"));

        // geometry: GeoJSON -> Sedona geometry
        Dataset<Row> withGeom = features
                .withColumn("geometry_json", to_json(col("f.geometry")))
                .withColumn("geometry", expr("ST_GeomFromGeoJSON(geometry_json)"));

        // expand properties.* into normal columns + keep geometry
        return withGeom.select(
                col("geometry"),
                col("f.properties.*")
        );
    }

    /** Read shapefile directory (folder that contains .shp/.dbf/.shx/...) */
    public static Dataset<Row> readCommunities(SparkSession spark, String shapefileFolderPath) {

        Path dir = Paths.get(shapefileFolderPath);
        if (!Files.exists(dir)) {
            throw new RuntimeException("Communities folder NOT FOUND:\n" + dir.toAbsolutePath());
        }

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // IMPORTANT: pass the DIRECTORY path (NOT the .shp file)
        SpatialRDD<?> spatialRDD = ShapefileReader.readToGeometryRDD(jsc, dir.toAbsolutePath().toString());

        return Adapter.toDf(spatialRDD, spark);
    }

    /** Spatial join: polygon contains point */
    public static Dataset<Row> spatiallyJoin(Dataset<Row> gbifDf, Dataset<Row> communitiesDf) {

        // avoid geometry name collision
        Dataset<Row> c = communitiesDf.withColumnRenamed("geometry", "community_geometry");

        Dataset<Row> joined = gbifDf.join(
                c,
                expr("ST_Contains(community_geometry, geometry)"),
                "inner"
        );

        // keep GBIF point geometry, drop polygon geometry
        return joined.drop("community_geometry");
    }

    /** Write to MongoDB */
    public static void writeJoinedData(Dataset<Row> joinedDf) {

        Dataset<Row> out = joinedDf
                .withColumn("geometry", expr("ST_AsGeoJSON(geometry)")); // store as GeoJSON string

        out.write()
                .format("mongodb")
                .mode("overwrite")
                .save();
    }

    /** ✅ MAIN: run this class only */
    public static void main(String[] args) {

        // ---- CHANGE THESE TWO PATHS to match your real files ----
        String gbifGeoJson = "C:\\Users\\hani\\Desktop\\UNI\\win2025-26\\No SQL\\Project\\Project-2\\Project2CodeTemplate\\target\\classes\\project2\\flora_germany.geojson";
        String communitiesFolder = "C:\\Users\\hani\\Desktop\\UNI\\win2025-26\\No SQL\\Project\\Project-2\\Project2CodeTemplate\\target\\classes\\communities";
        // --------------------------------------------------------

        SparkSession spark = getSedonaMongoSession();

        System.out.println("GeoJSON = " + gbifGeoJson);
        System.out.println("Communities folder = " + communitiesFolder);

        System.out.println("Reading GBIF...");
        Dataset<Row> gbif = readGbif(spark, gbifGeoJson);
        System.out.println("GBIF count = " + gbif.count());
        gbif.show(3, false);

        System.out.println("Reading communities...");
        Dataset<Row> comm = readCommunities(spark, communitiesFolder);
        System.out.println("Communities count = " + comm.count());
        comm.show(3, false);

        System.out.println("Spatial join...");
        Dataset<Row> joined = spatiallyJoin(gbif, comm);
        System.out.println("Joined count = " + joined.count());
        joined.show(5, false);

        System.out.println("Writing to MongoDB " + MONGO_DB + "." + MONGO_COLLECTION + " ...");
        writeJoinedData(joined);

        System.out.println("DONE ✅ Check in mongosh:");
        System.out.println("use biodiv");
        System.out.println("db.gbif.countDocuments()");
        spark.stop();
    }
}
