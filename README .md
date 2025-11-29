 Versioned Collection & Frugal Skiplist Project

 
Winter Semester 2025/2026 — NoSQL Project

---

## 📌 1. Project Goal

The goal of this project is to implement a **multi-versioned data structure** that supports:

- **Appending values** with a version (timestamp)  
- **Querying the visible value** at a given timestamp  
- **Two storage modes**:
  - In-memory (volatile)
  - Persistent (KVStore + JSON)
- **Two data structures**:
  1. Simple linked list (VLinkedList)
  2. Frugal Skiplist (optimized version list)

---

## 📌 2. Implemented Classes

### ✅ 2.1 `VLinkedList<P>`
Simple in-memory version list:
- New versions become the head.
- Linked list sorted by timestamp (newest → oldest).
- `findVisible()` walks the list linearly.

### Complexity
- Append: **O(1)**
- FindVisible: **O(n)**

---

### ✅ 2.2 `BackedVLinkedList<P>`
Persistent version list stored in a **KVStore**:
- Stores nodes as JSON via Jackson.
- Uses `Serializer<P>` for payloads.
- Each node keeps:
  - timestamp  
  - payload  
  - nextK  

---

### ✅ 2.3 `FrugalSkiplist<P>`
Optimized in-memory skiplist version list:
- Each node has:
  - `level`
  - `vRidgy` skip pointer
  - `next` pointer
- Fast search using skip pointers.

### Complexity
- Append: **O(log n)**  
- FindVisible: **O(log n)** average

---

### ✅ 2.4 `BackedFrugalSkiplist<P>`
Persistent version of FrugalSkiplist:
- Stores node records in KVStore.
- JSON fields:
  - timestamp  
  - payload  
  - level  
  - nextK  
  - vSkip  

---

## 📌 3. Supporting Components

### ✔ `Serializer<T>`
Converts values to/from String.  
Used for storing payloads inside JSON.

### ✔ `KVStore`
Simple key-value storage interface (`put`, `get`).

### ✔ `FlushableKVStore`
Extends KVStore with `flushDB()`.

### ✔ `InMemoryKVStore`
Custom HashMap-based KVStore created for testing.

---

## 📌 4. Manual Testing (ManualTest.java)

We manually tested all 4 structures.

Inserted versions:

```
("ten", 2)
("five", 7)
("ten", 8)
```

Tested:

```
findVisible(1)   = null
findVisible(2)   = ten
findVisible(6)   = ten
findVisible(7)   = five
findVisible(8)   = ten
findVisible(100) = ten
```

All implementations returned correct results.

---

## 📌 5. Jackson Integration

We added these JARs (same version):

```
jackson-core-2.15.0.jar
jackson-annotations-2.15.0.jar
jackson-databind-2.15.0.jar
```

Used for serializing NodeRecord objects to JSON.

---

## 📌 6. Summary

✔ Built 4 versioned data structures  
✔ Implemented skiplist (fast) + linked list (simple)  
✔ Added persistent variants using KVStore  
✔ Created InMemoryKVStore for testing  
✔ All logic tested via ManualTest  
✔ Ready for MultiVersionMap integration  

---

## 📌 7. Next Steps

- Integrate into VersionListFactory  
- Implement MultiVersionMap  
- Benchmark performance  
- Add higher-level MVCC logic if required  

---


## 8.  Task 1.3 – A Simple Multi-Version Map
- In this task, we implement a simple MultiVersionMap using a Java TreeMap that maps keys to versioned data structures. 
- Each key is associated with a VersionList storing multiple versions of a payload. The MultiVersionMap supports:
  - inserting new versions 
  - retrieving the visible version at a given timestamp 
  - performing range snapshots 
  - running full-range snapshots 
  - The task consists of sub-parts 1.3(a), 1.3(b), and 1.3(c).
  
--- 
 ##   📌 1.3(a) – Implementation of BackedSimpleMVM<K,P>
1. We implemented:

 _BackedSimpleMVM<K extends Comparable<? super K>, P>_
    which implements the MultiVersionMap interface.
    
2. Internal Structure

_Uses a TreeMap<K, VersionList<P>> to maintain keys in sorted order.
Uses a VersionListFactory<P> to create appropriate version lists:_

* BackedVLinkedList<P>
* BackedFrugalSkiplist<P>
* Stores data in a KVStore (here: InMemoryKVStore). 
* Maintains a global version counter, starting at version 1, incremented at each append.

  Implemented Methods
    - long append(K key, P payload)
 Assigns the next version number and appends the payload to the appropriate VersionList.
    Map.Entry<K,P> get(K key, long timestamp)
    Returns the version of the record visible at timestamp t.
    Iterator<Map.Entry<K,P>> rangeSnapshot(...)
    Returns visible versions for keys within [low, high].
    Iterator<Map.Entry<K,P>> snapshot(long timestamp)
    Returns visible versions for all keys.
  
---
    
## 📌 1.3(b) – Test Class Completion
We completed the provided Test.java as required.
*     ✔ Steps Implemented
*     Read the dataset test_data.csv using the provided readData() method.
*     Insert all entries into two MultiVersionMaps:
*     one using BackedVLinkedList
*     one using BackedFrugalSkiplist
*     Perform the required Range-Snapshot:
*     Range: KEY002 → KEY004  (inclusive)
*     Timestamp: 20
    

* Correct Output (matches assignment)
    From the project PDF, the expected visible versions are:
    _KEY002 = Payload[title=Some Title for KEY002, comment=Change 3 for key KEY002, timestamp=19]
    KEY003 = Payload[title=Some Title for KEY003, comment=Change 4 for key KEY003, timestamp=20]
    KEY004 = Payload[title=Some Title for KEY004, comment=Change 3 for key KEY004, timestamp=13]_

    Our implementation produced exactly this output for both:
    _BackedSimpleMVM + BackedVLinkedList
    BackedSimpleMVM + BackedFrugalSkiplist_
    thus validating correctness.

---

## 1.3(c) – Runtime Benchmarking

We extended the Test class to benchmark both MultiVersionMap variants using benchmark_data.csv (~500k entries). 
We measured:

1. Insertion Time
    Average measured over 2 runs:
                VersionList Type	    Avg Insertion Time
                 BackedVLinkedList	    ~0.39 s
                 BackedFrugalSkiplist	~4.86 s
    
Explanation:
    Linked list append is O(1) → faster.
    Skiplist append is O(log n) → slower due to multiple levels and pointer maintenance.
    
2. Snapshot Performance
        We executed full-range snapshots at timestamps:
        10, 100, 500, 1,000, 5,000,
        10,000, 50,000, 100,000, 500,000
        
Results (typical averages):
    Timestamp	VLinkedList (ns)	FrugalSkiplist (ns)
    10	        380M	            22M
    100	        406M	            24M
    500	        401M	            24M
    1,000	    421M	            25M
    5,000	    381M	            23M
    …	…	…
    500,000	~650k	~790k
    

✔ Explanation
    Linked List snapshot is O(n) → slow
    Frugal Skiplist snapshot is O(log n) → much faster
    At very high timestamps, early termination leads to smaller times for both
---


