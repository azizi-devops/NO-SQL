package project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HelloMongoDB {

    public static void main(String[] args) {

        String mongoUri = "mongodb://localhost:27017";
        String db = "biodiv";
        String coll = "gbif";

        SparkSession spark = SparkSession.builder()
                .appName("HelloMongoDB")
                .master("local[*]")
                .config("spark.mongodb.read.connection.uri", mongoUri)
                .config("spark.mongodb.read.database", db)
                .config("spark.mongodb.read.collection", coll)
                .getOrCreate();

        System.out.println("Reading from MongoDB...");

        Dataset<Row> df = spark.read()
                .format("mongodb")
                .load();

        df.printSchema();
        df.show(5, false);

        System.out.println("Count = " + df.count());

        spark.stop();
    }
}