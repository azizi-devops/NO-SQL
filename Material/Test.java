import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Test {

    public record Payload(String title, String comment, String timestamp) {}

    public interface FlushableKVStore extends KVStore {
        void flushDB();
    }

    public static void main(String[] args) {

        // Reading CSV data
        List<Map.Entry<String, Payload>> data = readData("NoSQL_Exercise_Sheet01/Material/test_data.csv");

        // Serializer for Payload

        Serializer<Payload> serializer = new Serializer<Payload>() {
            @Override
            public String serialize(Payload p) {
                return p.comment();          // Only store comment
            }

            @Override
            public Payload deSerialize(String s) {
                return new Payload("", s, ""); // Comment is stored, title/timestamp not needed
            }
        };

        // Stores

        KVStore storeLinked = new InMemoryKVStore();
        KVStore storeSkip = new InMemoryKVStore();

        // Factories


        VersionListFactory<Payload> linkedFactory = (store, ser) -> new BackedVLinkedList<>(store, ser);

        VersionListFactory<Payload> skipFactory = (store, ser) -> new BackedFrugalSkiplist<>(store, ser);

        // MultiVersionMap Instances

        BackedSimpleMVM<String, Payload> mvmLinked = new BackedSimpleMVM<>(linkedFactory, storeLinked, serializer);

        BackedSimpleMVM<String, Payload> mvmSkip = new BackedSimpleMVM<>(skipFactory, storeSkip, serializer);

        // Insert Records (CSV timestamp ignored)

        for (Map.Entry<String, Payload> entry : data) {
            mvmLinked.append(entry.getKey(), entry.getValue());
            mvmSkip.append(entry.getKey(), entry.getValue());
        }

        // RANGE SNAPSHOT (KEY002..KEY004 @ timestamp 20)

        long timestamp = 20;

        System.out.println("BackedVLinkedList Results ");
        var it1 = mvmLinked.rangeSnapshot("KEY002", true, "KEY004", true, timestamp);
        while (it1.hasNext()) {
            var e = it1.next();
            System.out.println(e.getKey() + "=" + e.getValue());
        }

        System.out.println("\nBackedFrugalSkiplist Results ");
        var it2 = mvmSkip.rangeSnapshot("KEY002", true, "KEY004", true, timestamp);
        while (it2.hasNext()) {
            var e = it2.next();
            System.out.println(e.getKey() + "=" + e.getValue());
        }
        //  TASK 1.3(c) — BENCHMARKING

        System.out.println("\n\n BENCHMARKING \n");

        List<Map.Entry<String, Payload>> benchData = readData("NoSQL_Exercise_Sheet01/Material/benchmark_data.csv");

        // Timestamps to test
        long[] timestamps = {10, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000};

        int RUNS = 5;   // repeat tests for averaging

        System.out.println("Benchmarking on " + benchData.size() + " rows...\n");


        // Benchmark insertion

        long totalInsertLinked = 0;
        long totalInsertSkip = 0;

        for (int r = 0; r < RUNS; r++) {

            // Fresh KVStores each run
            KVStore s1 = new InMemoryKVStore();
            KVStore s2 = new InMemoryKVStore();

            BackedSimpleMVM<String, Payload> mvml =new BackedSimpleMVM<>(linkedFactory, s1, serializer);
            BackedSimpleMVM<String, Payload> mvms = new BackedSimpleMVM<>(skipFactory, s2, serializer);

            // Measure VLinkedList insertion
            long t1 = System.nanoTime();
            for (var e : benchData) {
                mvml.append(e.getKey(), e.getValue());
            }
            long t2 = System.nanoTime();
            totalInsertLinked += (t2 - t1);

            // Measure Skiplist insertion
            long t3 = System.nanoTime();
            for (var e : benchData) {
                mvms.append(e.getKey(), e.getValue());
            }
            long t4 = System.nanoTime();
            totalInsertSkip += (t4 - t3);
        }

        long avgInsertLinked = totalInsertLinked / RUNS;
        long avgInsertSkip = totalInsertSkip / RUNS;

        System.out.println("Average Insertion Times (ns):");
        System.out.println("VLinkedList     : " + avgInsertLinked);
        System.out.println("FrugalSkiplist  : " + avgInsertSkip);


        // Benchmark snapshots

        System.out.println("\nSnapshot Benchmark:");

        // Prepare fresh objects for snapshot tests
        KVStore s1 = new InMemoryKVStore();
        KVStore s2 = new InMemoryKVStore();

        BackedSimpleMVM<String, Payload> mvml = new BackedSimpleMVM<>(linkedFactory, s1, serializer);
        BackedSimpleMVM<String, Payload> mvms = new BackedSimpleMVM<>(skipFactory, s2, serializer);

        // Insert once
        for (var e : benchData) {
            mvml.append(e.getKey(), e.getValue());
            mvms.append(e.getKey(), e.getValue());
        }

        // Run snapshot benchmarks
        for (long ts : timestamps) {

            long totalSnapLinked = 0;
            long totalSnapSkip = 0;

            for (int r = 0; r < RUNS; r++) {

                long a = System.nanoTime();
                mvml.snapshot(ts).hasNext();   // just triggering iteration
                long b = System.nanoTime();
                totalSnapLinked += (b - a);

                long c = System.nanoTime();
                mvms.snapshot(ts).hasNext();
                long d = System.nanoTime();
                totalSnapSkip += (d - c);
            }

            System.out.println("Timestamp " + ts + ":");
            System.out.println("VLinkedList    avg = " + (totalSnapLinked / RUNS));
            System.out.println("FrugalSkiplist avg = " + (totalSnapSkip / RUNS));
        }

        System.out.println("\n Benchmark Completed ");

        System.out.println("\n\n VWEAVER BENCHMARKING \n");

        long[] tsArray = {10, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000};
        int RUNS_VW = 5;

        VersionListFactory<Payload> weaverFactory =
                (store, ser) -> new BackedFrugalSkiplist<>(store, ser);

        long totalWeaverInsert = 0;

        for (int r = 0; r < RUNS_VW; r++) {
            KVStore sW = new InMemoryKVStore();
            BackedVWeaverMVM<String, Payload> mvmVW =
                    new BackedVWeaverMVM<>(weaverFactory, sW, serializer);

            long tStart = System.nanoTime();
            for (var e : benchData) {
                mvmVW.append(e.getKey(), e.getValue());
            }
            long tEnd = System.nanoTime();
            totalWeaverInsert += (tEnd - tStart);
        }

        long avgWeaverInsert = totalWeaverInsert / RUNS_VW;

        System.out.println("Average Insertion Times (ns):");
        System.out.println("VLinkedList       : " + avgInsertLinked);
        System.out.println("FrugalSkiplist    : " + avgInsertSkip);
        System.out.println("VWeaver           : " + avgWeaverInsert);

        System.out.println("\nVWeaver Snapshot Benchmark:\n");

        KVStore sW = new InMemoryKVStore();
        BackedVWeaverMVM<String, Payload> mvmVW =
                new BackedVWeaverMVM<>(weaverFactory, sW, serializer);

        for (var e : benchData) {
            mvmVW.append(e.getKey(), e.getValue());
        }

        for (long ts : tsArray) {
            long totalVW = 0;

            for (int r = 0; r < RUNS_VW; r++) {
                long a = System.nanoTime();
                mvmVW.snapshot(ts).hasNext();
                long b = System.nanoTime();
                totalVW += (b - a);
            }

            long avgVW = totalVW / RUNS_VW;

            System.out.println("Timestamp " + ts + ":");
            System.out.println("  VWeaver avg      = " + avgVW);
        }

        System.out.println("\n VWeaver Benchmark Completed.\n");


    }

    // Provided CSV reader

    public static List<Map.Entry<String, Payload>> readData(String path) {
        List<Map.Entry<String, Payload>> l = new ArrayList<>();
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            // Skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", 4); // key,title,comment,timestamp
                String key = values[0];
                String title = values[1];
                String comment = values[2];
                String timestamp = values[3];

                l.add(new AbstractMap.SimpleEntry<>(key,
                        new Payload(title, comment, timestamp)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return l;
    }
}
