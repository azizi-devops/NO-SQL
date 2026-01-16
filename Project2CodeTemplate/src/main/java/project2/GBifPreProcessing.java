package project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;

public class GBifPreProcessing {

    // ---- CHANGE THESE ONLY IF NEEDED ----
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "biodiv";
    private static final String COLLECTION = "gbif";

    // Your current file location (works if Maven copied it)
    private static final String GEOJSON_RELATIVE_PATH = "target/classes/project2/flora_germany.geojson";

    public static void main(String[] args) throws Exception {

        // 0) Fix Hadoop warning on Windows (not always fatal, but prevents some crashes)
        // Creates %TEMP%/bin so Hadoop doesn't throw "bin directory does not exist"
        String tmpDir = System.getProperty("java.io.tmpdir");
        Path hadoopBin = Path.of(tmpDir, "bin");
        Files.createDirectories(hadoopBin);
        System.setProperty("hadoop.home.dir", tmpDir);

        // 1) Resolve GeoJSON path
        File geoJsonFile = new File(GEOJSON_RELATIVE_PATH);
        if (!geoJsonFile.exists()) {
            throw new RuntimeException(
                    "GeoJSON file NOT FOUND at: " + geoJsonFile.getAbsolutePath() + "\n" +
                            "Fix: put it into src/main/resources/project2/ and run 'mvn package'."
            );
        }
        String geoJsonPath = geoJsonFile.getAbsolutePath();

        // 2) Create Spark session + Mongo connector config
        SparkSession spark = SparkSession.builder()
                .appName("GBIF-PreProcessing")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.mongodb.write.connection.uri", MONGO_URI)
                .getOrCreate();

        try {
            System.out.println("GeoJSON PATH = " + geoJsonPath);

            // 3) Read GeoJSON correctly (IMPORTANT: multiline=true)
            Dataset<Row> raw = spark.read()
                    .option("multiline", "true")
                    .option("mode", "PERMISSIVE")
                    .json(geoJsonPath);

            // Debug: show schema quickly
            System.out.println("RAW SCHEMA:");
            raw.printSchema();

            // 4) Explode features
            // GeoJSON FeatureCollection shape: { "type": "...", "features": [ ... ] }
            Dataset<Row> features = raw
                    .select(explode(col("features")).alias("feature"))
                    .select("feature.*"); // now we have geometry + properties

            // 5) Extract fields
            Dataset<Row> gbifDf = features
                    .withColumn("order", col("properties.order"))
                    .withColumn("family", col("properties.family"))
                    .withColumn("genus", col("properties.genus"))
                    .withColumn("species", col("properties.species"))
                    .withColumn("lon", col("geometry.coordinates").getItem(0).cast("double"))
                    .withColumn("lat", col("geometry.coordinates").getItem(1).cast("double"))
                    .drop("properties")
                    .drop("geometry");

            // Optional: remove empty rows
            gbifDf = gbifDf.filter(col("species").isNotNull());

            long count = gbifDf.count();
            System.out.println("GBIF rows ready to write = " + count);

            gbifDf.show(5, false);

            // 6) Write to MongoDB (biodiv.gbif)
            gbifDf.write()
                    .format("mongodb")
                    .mode("append")
                    .option("database", DB_NAME)
                    .option("collection", COLLECTION)
                    .save();

            System.out.println("DONE. Written to MongoDB: " + DB_NAME + "." + COLLECTION);

        } finally {
            spark.stop();
        }
    }
}
