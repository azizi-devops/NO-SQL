import java.util.*;
import java.util.AbstractMap.SimpleEntry;

public class BackedVWeaverMVM<K extends Comparable<K>, P>
        implements MultiVersionMap<K, P> {

    // -------------- fields --------------
    private final TreeMap<K, BackedFrugalSkiplist<P>> map = new TreeMap<>();
    private final VersionListFactory<P> factory;
    private final KVStore store;
    private final Serializer<P> serializer;
    private long versionCounter = 1;


    // -------------- constructor --------------
    public BackedVWeaverMVM(VersionListFactory<P> factory,
                            KVStore store,
                            Serializer<P> serializer) {
        this.factory = factory;
        this.store = store;
        this.serializer = serializer;
    }


    // -------------- append (returns long) --------------
    @Override
    public long append(K key, P payload) {

        // manual get-or-create (FIXES THE ERROR)
        BackedFrugalSkiplist<P> list = map.get(key);
        if (list == null) {
            list = (BackedFrugalSkiplist<P>) factory.create(store, serializer);
            map.put(key, list);
        }

        long ts = versionCounter++;

        String thisNodeKey = list.appendGetKey(payload, ts);

        K nextK = map.higherKey(key);
        if (nextK == null)
            return ts;

        BackedFrugalSkiplist<P> nextList = map.get(nextK);

        BackedFrugalSkiplist.NodeRecord candidate =
                nextList.FindVisibleNodeRecord(ts);

        BackedFrugalSkiplist.NodeRecord thisNode =
                list.FindVisibleNodeRecord(ts);

        if (thisNode != null) {
            if (candidate != null)
                thisNode.kSkip = Long.toString(candidate.timestamp);
            else
                thisNode.kSkip = null;

            list.storeNode(thisNodeKey, thisNode);
        }

        return ts;
    }


    // -------------- get --------------
    @Override
    public Map.Entry<K, P> get(K key, long timestamp) {

        BackedFrugalSkiplist<P> list = map.get(key);
        if (list == null)
            return null;

        P result = list.findVisible(timestamp);
        if (result == null)
            return null;

        return new SimpleEntry<>(key, result);
    }


    // -------------- rangeSnapshot (VWeaver) --------------
    @Override
    public Iterator<Map.Entry<K, P>> rangeSnapshot(
            K fromKey, boolean fromInclusive,
            K toKey, boolean toInclusive,
            long timestamp) {

        List<Map.Entry<K, P>> result = new ArrayList<>();

        K start = fromKey;
        if (!fromInclusive) {
            start = map.higherKey(fromKey);
            if (start == null) return result.iterator();
        }

        K end = toKey;
        if (!toInclusive) {
            end = map.lowerKey(toKey);
            if (end == null) return result.iterator();
        }

        K currentKey = start;
        BackedFrugalSkiplist<P> currentList = map.get(currentKey);

        if (currentList == null)
            return result.iterator();

        BackedFrugalSkiplist.NodeRecord currentNode =
                currentList.FindVisibleNodeRecord(timestamp);

        if (currentNode != null)
            result.add(new SimpleEntry<>(currentKey,
                    currentList.getSerializer().deSerialize(currentNode.payload)));
        else
            result.add(new SimpleEntry<>(currentKey, null));

        K nextK = map.higherKey(currentKey);

        while (nextK != null && nextK.compareTo(end) <= 0) {

            BackedFrugalSkiplist<P> nextList = map.get(nextK);

            BackedFrugalSkiplist.NodeRecord nextNode = null;

            if (currentNode != null && currentNode.kSkip != null) {
                long jump = Long.parseLong(currentNode.kSkip);
                nextNode = nextList.FindVisibleNodeRecord(jump);
            }

            if (nextNode == null)
                nextNode = nextList.FindVisibleNodeRecord(timestamp);

            if (nextNode != null)
                result.add(new SimpleEntry<>(nextK,
                        nextList.getSerializer().deSerialize(nextNode.payload)));
            else
                result.add(new SimpleEntry<>(nextK, null));

            currentNode = nextNode;
            currentKey = nextK;
            nextK = map.higherKey(currentKey);
        }

        return result.iterator();
    }


    // -------------- snapshot (full-range) --------------
    @Override
    public Iterator<Map.Entry<K, P>> snapshot(long timestamp) {

        List<Map.Entry<K, P>> out = new ArrayList<>();

        for (K key : map.keySet()) {
            BackedFrugalSkiplist<P> list = map.get(key);
            BackedFrugalSkiplist.NodeRecord node =
                    list.FindVisibleNodeRecord(timestamp);

            if (node != null)
                out.add(new SimpleEntry<>(key,
                        list.getSerializer().deSerialize(node.payload)));
            else
                out.add(new SimpleEntry<>(key, null));
        }

        return out.iterator();
    }
}
