package project2;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.*;

public class GBifAnalytics {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB = "biodiv";
    private static final String COL = "gbif";

    /**
     * Try to guess which column is the community name.
     * Common in German community shapefiles: GEN, NAME, name
     */
    private static String detectCommunityNameField(Set<String> fields) {
        List<String> candidates = Arrays.asList("GEN", "NAME", "name", "community", "gemeinde", "Community");
        for (String c : candidates) {
            if (fields.contains(c)) return c;
        }
        // fallback: choose first non-geometry string-ish field name you see
        // (you can hardcode later once you know it)
        return null;
    }

    /**
     * (f) Use ONLY MongoDB aggregation:
     * Find the 10 communities with the highest number of DISTINCT species.
     * Print: community-name + number-of-distinct-species.
     *
     * Uses DB=biodiv, collection=gbif (required by task). :contentReference[oaicite:5]{index=5}
     */
    public static void mostDiverseCommunities() {

        try (MongoClient client = MongoClients.create(MONGO_URI)) {
            MongoCollection<Document> gbif = client.getDatabase(DB).getCollection(COL);

            // --- You MUST set this to the real community name field once you know it ---
            // If you already know it’s "GEN", just replace it with "GEN".
            // Otherwise: open ONE document in MongoDB Compass and look at the field names.
            String communityField = "GEN";   // <-- change if needed
            String speciesField = "species";

            List<Document> pipeline = Arrays.asList(
                    // group by community and collect distinct species
                    new Document("$group", new Document("_id", "$" + communityField)
                            .append("speciesSet", new Document("$addToSet", "$" + speciesField))
                    ),
                    // compute size of set
                    new Document("$project", new Document("community", "$_id")
                            .append("_id", 0)
                            .append("distinctSpecies", new Document("$size", "$speciesSet"))
                    ),
                    // sort desc
                    new Document("$sort", new Document("distinctSpecies", -1)),
                    // top 10
                    new Document("$limit", 10)
            );

            System.out.println("Top 10 most diverse communities:");
            for (Document d : gbif.aggregate(pipeline)) {
                System.out.println(d.getString("community") + " -> " + d.getInteger("distinctSpecies"));
            }

            /*
             MongoDB Shell version (mongosh):

             db.gbif.aggregate([
               { $group: { _id: "$GEN", speciesSet: { $addToSet: "$species" } } },
               { $project: { _id: 0, community: "$_id", distinctSpecies: { $size: "$speciesSet" } } },
               { $sort: { distinctSpecies: -1 } },
               { $limit: 10 }
             ])
            */
        }
    }

    /**
     * (g) Use Spark + Mongo connector:
     * - Retrieve the same top 10 communities + their distinct species-set
     * - Compute Jaccard similarity for every pair
     * - Print top 10 most similar pairs
     */
    public static void mostSimilarCommunities() {

        // Build Spark session with Mongo access
        SparkSession spark = SparkSession.builder()
                .appName("GBIF-Mongo-Similarity")
                .master("local[*]")
                .config("spark.mongodb.read.connection.uri", MONGO_URI)
                .config("spark.mongodb.read.database", DB)
                .config("spark.mongodb.read.collection", COL)
                .getOrCreate();

        // --- You MUST set this to the real community name field once you know it ---
        String communityField = "GEN";  // <-- change if needed
        String speciesField = "species";

        // Mongo aggregation pipeline: return top 10 communities with species array
        String pipelineJson = String.format(
                "[ " +
                        " { $group: { _id: '$%s', speciesSet: { $addToSet: '$%s' } } }, " +
                        " { $project: { _id: 0, community: '$_id', speciesSet: 1, distinctSpecies: { $size: '$speciesSet' } } }, " +
                        " { $sort: { distinctSpecies: -1 } }, " +
                        " { $limit: 10 } " +
                        "]",
                communityField, speciesField
        );

        Dataset<Row> top10 = spark.read()
                .format("mongodb")
                .option("aggregation.pipeline", pipelineJson)
                .load();

        // Ensure we have the expected schema
        // top10: community (string), speciesSet (array<string>), distinctSpecies (int)
        top10.cache();

        // Self-join to create all pairs (a,b) with a.community < b.community
        Dataset<Row> a = top10.alias("a");
        Dataset<Row> b = top10.alias("b");

        Dataset<Row> pairs = a.crossJoin(b)
                .where(col("a.community").lt(col("b.community")))
                .select(
                        col("a.community").alias("c1"),
                        col("b.community").alias("c2"),
                        col("a.speciesSet").alias("s1"),
                        col("b.speciesSet").alias("s2")
                )
                .withColumn("intersection", array_intersect(col("s1"), col("s2")))
                .withColumn("union", array_union(col("s1"), col("s2")))
                .withColumn("jaccard",
                        size(col("intersection")).cast("double")
                                .divide(size(col("union")).cast("double"))
                )
                .select("c1", "c2", "jaccard")
                .orderBy(col("jaccard").desc())
                .limit(10);

        System.out.println("Top 10 most similar community pairs (Jaccard):");
        pairs.show(false);

        spark.stop();
    }
}
