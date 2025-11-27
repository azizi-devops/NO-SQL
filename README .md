# README — Versioned Collection & Frugal Skiplist Project

## 👥 Project Members
**Hani Karim Azizi** + Group members  
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
  - nextKey  

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
  - nextKey  
  - vRidgyKey  

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

## 📞 Contact

For project questions, contact **Hani Karim Azizi**.
