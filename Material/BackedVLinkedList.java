// package ...;  // <-- copy the package from VersionList/JacksonExample
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BackedVLinkedList<P> implements VersionList<P> {

    // JSON representation of one node in the KVStore
    private static class NodeRecord {
        public long timestamp;
        public String payload;
        public String nextK;

        public NodeRecord() {}  // needed by Jackson
    }

    private final KVStore store;
    private final Serializer<P> serializer;
    private final ObjectMapper mapper = new ObjectMapper();

    private String headKey = null;

    public BackedVLinkedList(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
    }

    @Override
    public void append(P p, long timestamp) {
        try {
            NodeRecord record = new NodeRecord();
            record.timestamp = timestamp;
            record.payload = serializer.serialize(p);
            record.nextK = headKey;

            String key = Long.toString(timestamp);
            String json = mapper.writeValueAsString(record);

            store.put(key, json);
            headKey = key;

        } catch (Exception e) {
            throw new RuntimeException("Failed to append to BackedVLinkedList", e);
        }
    }

    @Override
    public P findVisible(long timestamp) {
        try {
            String currentKey = headKey;

            while (currentKey != null) {
                String json = store.get(currentKey);
                if (json == null) break;

                NodeRecord record = mapper.readValue(json, NodeRecord.class);

                if (record.timestamp <= timestamp) {
                    return serializer.deSerialize(record.payload);
                }

                currentKey = record.nextK;
            }

            return null;

        } catch (Exception e) {
            throw new RuntimeException("Failed to read from BackedVLinkedList", e);
        }
    }
}
