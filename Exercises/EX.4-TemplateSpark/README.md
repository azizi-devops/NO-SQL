# Exercise 4 – NoSQL Database Systems (Spark, Java)
  
Course: NoSQL-Database Systems  
Semester: Wintersemester 2025  
Exercise: Sheet 04 – MapReduce & Spark DataFrames

## Project structure

- `pom.xml` – Maven configuration with Spark dependencies
- `src/main/java/spark/MapReduceSpark.java`
  - `wordCount()` – Task 4.1 a  
  - `longestWords()` – Task 4.1 b  
  - `averageWordLength()` – Task 4.1 c  
  - `anagramm()` – Task 4.1 d  
  - `consecutiveWords()` – Task 4.1 e  

- `src/main/java/spark/DataFrameAnalytics.java`
  - `averageMass()` – Task 4.2 b  
  - `fallClassification()` – Task 4.2 c  
  - `noCoordinates()` – Task 4.2 d  
  - `dataCleaning()` – Task 4.2 e  
  - `pointDistance()` – Task 4.2 f  
  - `addDataPoints()` – Task 4.2 g  

- `src/main/java/spark/GeoLocation.java`
  - POJO with fields `type`, `longitude`, `latitude` used in Task 4.2 g

- `src/main/resources/words.txt` – input for Task 4.1  
- `src/main/resources/meteorite.json` – input for Task 4.2 (NASA meteorite data)

## How to run

From IntelliJ:

1. Import project as **Maven** project.
2. Run `spark.MapReduceSpark` for Task 4.1.
3. Run `spark.DataFrameAnalytics` for Task 4.2.

From command line:

```bash
mvn package
# Task 4.1
mvn exec:java -Dexec.mainClass="spark.MapReduceSpark"
# Task 4.2
mvn exec:java -Dexec.mainClass="spark.DataFrameAnalytics"
