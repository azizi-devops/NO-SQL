package project2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class GeoAnalysis {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "biodiv";
    private static final String MONGO_COLLECTION = "gbif";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("GeoAnalysis")
                .master("local[*]")
                .config("spark.mongodb.read.connection.uri", MONGO_URI)
                .config("spark.mongodb.read.database", MONGO_DB)
                .config("spark.mongodb.read.collection", MONGO_COLLECTION)
                .getOrCreate();

        Dataset<Row> df = readFromMongoDB(spark);

        // You can keep this during debugging (then remove later)
        df.printSchema();

        String communityCol = detectCommunityColumn(df);
        String speciesCol = detectSpeciesColumn(df);

        // 10 communities with highest number of distinct species
        Dataset<Row> top10 = df
                .filter(col(communityCol).isNotNull().and(col(speciesCol).isNotNull()))
                .groupBy(col(communityCol).alias("community"))
                .agg(countDistinct(col(speciesCol)).alias("distinct_species"))
                .orderBy(col("distinct_species").desc(), col("community").asc())
                .limit(10);

        // Print only community + distinct_species
        top10.show(10, false);

        spark.stop();
    }

    /** (g) Read from MongoDB collection created by preprocessing. */
    public static Dataset<Row> readFromMongoDB(SparkSession spark) {
        return spark.read()
                .format("mongodb")
                .load();
    }

    /** Try to guess which column is the community name/id. */
    private static String detectCommunityColumn(Dataset<Row> df) {
        Set<String> cols = new HashSet<>(Arrays.asList(df.columns()));

        // common shapefile/name columns
        String[] candidates = new String[]{
                "community", "Community",
                "name", "Name", "NAME",
                "gemeinde", "Gemeinde", "GEMEINDE",
                "ort", "Ort", "ORT",
                "id", "ID", "Id"
        };

        for (String c : candidates) {
            if (cols.contains(c)) return c;
        }

        // If your preprocessing renamed the polygon attributes with a prefix, try these patterns
        for (String c : df.columns()) {
            String lc = c.toLowerCase();
            if (lc.contains("name") || lc.contains("community") || lc.contains("gemeinde")) {
                return c;
            }
        }

        throw new RuntimeException(
                "Cannot detect community column. Available columns: " + Arrays.toString(df.columns()) +
                        "\n=> Open your Mongo documents / schema and tell me which column represents the community name."
        );
    }

    /** Species column in your streamed schema is 'species'. */
    private static String detectSpeciesColumn(Dataset<Row> df) {
        Set<String> cols = new HashSet<>(Arrays.asList(df.columns()));
        if (cols.contains("species")) return "species";

        // fallback: try something containing species
        for (String c : df.columns()) {
            if (c.toLowerCase().contains("species")) return c;
        }

        throw new RuntimeException(
                "Cannot detect species column. Available columns: " + Arrays.toString(df.columns())
        );
    }
}
