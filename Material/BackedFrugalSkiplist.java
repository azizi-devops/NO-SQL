// package ...;   // (copy appropriate package header)

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * BackedFrugalSkiplist<P>
 * Implements VersionList<P> with skip pointers stored in KVStore.
 * Extended for VWeaver by adding kRidgyKey.
 *
 * Used in:
 * - Task 1.2: Frugal Skiplist
 * - Task 1.4: VWeaver (cross-list pointers)
 */
public class BackedFrugalSkiplist<P> implements VersionList<P> {


    //-------------------NODE RECORD (STORED IN KVSTORE) ---------------------

    static class NodeRecord {
        public long timestamp;    // version / timestamp
        public int level;         // skiplist level
        public String payload;    // serialized payload
        public String nextKey;    // pointer to next (older) node
        public String vRidgyKey;  // pointer within same skiplist (Task 1.2)
        public String kRidgyKey;  // NEW: pointer to next key’s version list (Task 1.4)

        public NodeRecord() {}
    }


    //----------------- FIELDS -------------------------
    private final KVStore store;
    private final Serializer<P> serializer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final java.util.Random rnd = new java.util.Random();

    // Pointer to the newest node in the skiplist
    private String headKey = null;



    // -----------------------CONSTRUCTOR ----------------------

    public BackedFrugalSkiplist(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
    }



    //------------ UTILITY: RANDOM LEVEL -------------------

    private int randomLevel() {
        int lvl = 0;
        while (rnd.nextBoolean()) lvl++;
        return lvl;
    }


    // ------------LOAD / SAVE NODE FROM KVSTORE-----------------

    private NodeRecord load(String key) throws Exception {
        if (key == null) return null;
        String json = store.get(key);
        if (json == null) return null;
        return mapper.readValue(json, NodeRecord.class);
    }

    private void save(String key, NodeRecord rec) throws Exception {
        store.put(key, mapper.writeValueAsString(rec));
    }

    //------Normal Append-----------

    @Override
    public void append(P p, long timestamp) {
        try {
            int lvl = randomLevel();
            String oldHeadKey = headKey;

            // Create node
            NodeRecord rec = new NodeRecord();
            rec.timestamp = timestamp;
            rec.level = lvl;
            rec.payload = serializer.serialize(p);
            rec.nextKey = oldHeadKey;
            rec.vRidgyKey = null;
            rec.kRidgyKey = null;   // must exist even if unused in Task 1.2

            // Set vRidgy pointer (skip pointer inside same list)
            String cursor = oldHeadKey;
            while (cursor != null) {
                NodeRecord c = load(cursor);
                if (c == null) break;
                if (c.level > lvl) {
                    rec.vRidgyKey = cursor;
                    break;
                }
                cursor = c.nextKey;
            }

            // Persist node
            String key = Long.toString(timestamp);
            save(key, rec);
            headKey = key;

        } catch (Exception e) {
            throw new RuntimeException("BackedFrugalSkiplist.append failed", e);
        }
    }



    // ----------------APPEND AND RETURN KEY---------------------
    // ----------------Required to set kRidgy in BackedVWeaverMVM------------------

    public String appendAndReturnKey(P p, long timestamp) {
        try {
            int lvl = randomLevel();
            String oldHeadKey = headKey;

            NodeRecord rec = new NodeRecord();
            rec.timestamp = timestamp;
            rec.level = lvl;
            rec.payload = serializer.serialize(p);
            rec.nextKey = oldHeadKey;
            rec.vRidgyKey = null;
            rec.kRidgyKey = null;

            // Set vRidgy pointer
            String cursor = oldHeadKey;
            while (cursor != null) {
                NodeRecord c = load(cursor);
                if (c == null) break;
                if (c.level > lvl) {
                    rec.vRidgyKey = cursor;
                    break;
                }
                cursor = c.nextKey;
            }

            // Save new node
            String key = Long.toString(timestamp);
            save(key, rec);
            headKey = key;

            return key;

        } catch (Exception e) {
            throw new RuntimeException("appendAndReturnKey failed", e);
        }
    }


    // ---------FIND VISIBLE PAYLOAD----------

    @Override
    public P findVisible(long timestamp) {
        try {
            String curKey = headKey;

            while (curKey != null) {
                NodeRecord cur = load(curKey);
                if (cur == null) break;

                // visible version found
                if (cur.timestamp <= timestamp) {
                    return serializer.deSerialize(cur.payload);
                }

                // try vRidgy pointer
                if (cur.vRidgyKey != null) {
                    NodeRecord skip = load(cur.vRidgyKey);
                    if (skip != null && skip.timestamp >= timestamp) {
                        curKey = cur.vRidgyKey;
                        continue;
                    }
                }

                // follow normal next pointer
                curKey = cur.nextKey;
            }

            return null;

        } catch (Exception e) {
            throw new RuntimeException("findVisible failed", e);
        }
    }



    //---------------FIND VISIBLE NODE RECORD----------------

    public NodeRecord findVisibleNodeRecord(long timestamp) {
        try {
            String curKey = headKey;

            while (curKey != null) {
                NodeRecord cur = load(curKey);
                if (cur == null) break;

                // first node <= timestamp
                if (cur.timestamp <= timestamp)
                    return cur;

                // try vRidgy pointer
                if (cur.vRidgyKey != null) {
                    NodeRecord skip = load(cur.vRidgyKey);
                    if (skip != null && skip.timestamp >= timestamp) {
                        curKey = cur.vRidgyKey;
                        continue;
                    }
                }

                // else step normally
                curKey = cur.nextKey;
            }

            return null;

        } catch (Exception e) {
            throw new RuntimeException("findVisibleNodeRecord failed", e);
        }
    }
    // -------------- public save wrapper (needed by VWeaver) --------------
    public void saveNode(String key, NodeRecord rec) {
        try {
            save(key, rec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // -------------- public load wrapper (needed by VWeaver) --------------
    public NodeRecord loadNode(String key) {
        try {
            return load(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    // -------------- public access: serializer --------------
    public Serializer<P> getSerializer() {
        return this.serializer;
    }


    //-------------UTILITY HELPER-------------
    public String keyOf(NodeRecord rec) {
        return Long.toString(rec.timestamp);
    }
}
