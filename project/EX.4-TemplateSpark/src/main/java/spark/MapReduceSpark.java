package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class MapReduceSpark {

    private static final Pattern SPACE = Pattern.compile("\\s+");
    private static final Pattern PUNCT = Pattern.compile("[,\\.?!;]");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapReduceSpark")
                .setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            String path = "src/main/resources/words.txt";
            JavaRDD<String> lines = sc.textFile(path);

            JavaRDD<String> words = toWords(lines);

            wordCount(words);
            longestWords(words);
            averageWordLength(words);
            anagramm(words);
            consecutiveWords(lines);
        }
    }

    /** Helper: line -> cleaned words (lowercase, punctuation removed) */
    private static JavaRDD<String> toWords(JavaRDD<String> lines) {
        return lines
                .flatMap(line -> {
                    // remove punctuation and split on whitespace
                    String cleaned = PUNCT.matcher(line).replaceAll("");
                    String[] parts = SPACE.split(cleaned.toLowerCase());
                    return Arrays.asList(parts).iterator();
                })
                .filter(w -> !w.isEmpty());
    }

    // 4.1 a) WordCount
    public static void wordCount(JavaRDD<String> words) {
        JavaPairRDD<String, Integer> wordOnes = words
                .mapToPair(w -> new Tuple2<>(w, 1));

        JavaPairRDD<String, Integer> counts = wordOnes
                .reduceByKey(Integer::sum);

        // sort by count descending
        List<Tuple2<String, Integer>> top10 = counts
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                .take(10);

        System.out.println("=== 4.1 a) WordCount – top 10 ===");
        top10.forEach(t -> System.out.println(t._1() + " -> " + t._2()));
    }

    // 4.1 b) 10 longest words
    public static void longestWords(JavaRDD<String> words) {
        JavaRDD<String> distinctWords = words.distinct();

        JavaPairRDD<Integer, String> byLength = distinctWords
                .mapToPair(w -> new Tuple2<>(w.length(), w));

        List<Tuple2<Integer, String>> longest = byLength
                .sortByKey(false)
                .take(10);

        System.out.println("=== 4.1 b) 10 longest words ===");
        longest.forEach(t -> System.out.println(t._2() + " (len=" + t._1() + ")"));
    }

    // 4.1 c) average word length
    public static void averageWordLength(JavaRDD<String> words) {
        // use mapToPair so we respect the exercise requirement
        Tuple2<Long, Long> sumAndCount = words
                .mapToPair(w -> new Tuple2<>(1L, (long) w.length()))
                .reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        double avg = (double) sumAndCount._2 / sumAndCount._1;

        System.out.println("=== 4.1 c) Average word length ===");
        System.out.println("Average length: " + avg);
    }

    // 4.1 d) anagram groups (only groups with >1 word)
    public static void anagramm(JavaRDD<String> words) {
        JavaPairRDD<String, String> keyed = words
                .mapToPair(w -> new Tuple2<>(sortedLetters(w), w));

        JavaPairRDD<String, Iterable<String>> groups = keyed.groupByKey();

        System.out.println("=== 4.1 d) Anagram groups (>1) ===");
        groups
                .filter(t -> {
                    int c = 0;
                    for (String ignored : t._2) {
                        c++;
                        if (c > 1) return true;
                    }
                    return false;
                })
                .collect()
                .forEach(t -> {
                    System.out.print(t._1() + " -> ");
                    StringBuilder sb = new StringBuilder();
                    for (String w : t._2()) {
                        if (sb.length() > 0) sb.append(", ");
                        sb.append(w);
                    }
                    System.out.println(sb);
                });
    }

    private static String sortedLetters(String w) {
        char[] chars = w.toCharArray();
        Arrays.sort(chars);
        return new String(chars);
    }

    // 4.1 e) bigrams (consecutive words in the text)
    public static void consecutiveWords(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> bigramOnes = lines
                .flatMapToPair(line -> {
                    String cleaned = PUNCT.matcher(line).replaceAll("");
                    String[] parts = SPACE.split(cleaned.toLowerCase());

                    new StringBuilder();
                    java.util.List<Tuple2<String, Integer>> res =
                            new java.util.ArrayList<>();

                    for (int i = 0; i < parts.length - 1; i++) {
                        String w1 = parts[i];
                        String w2 = parts[i + 1];
                        if (w1.isEmpty() || w2.isEmpty()) continue;
                        String bigram = w1 + " " + w2;
                        res.add(new Tuple2<>(bigram, 1));
                    }
                    return res.iterator();
                });

        JavaPairRDD<String, Integer> counts = bigramOnes
                .reduceByKey(Integer::sum);

        System.out.println("=== 4.1 e) Bigrams occurring > 10 times ===");
        counts
                .filter(t -> t._2() > 10)
                .collect()
                .forEach(t -> System.out.println("\"" + t._1() + "\" -> " + t._2()));
    }
}
