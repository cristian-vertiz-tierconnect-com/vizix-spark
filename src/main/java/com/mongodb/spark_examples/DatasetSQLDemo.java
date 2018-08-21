package com.mongodb.spark_examples;

import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

import java.util.HashMap;
import java.util.Map;


public final class DatasetSQLDemo {

    public static void main(final String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://admin:control123!@10.100.0.225/riot_main.thingSnapshots?authSource=admin")
                .config("spark.mongodb.output.uri", "mongodb://admin:control123!@10.100.0.225/riot_main.mergeResult?authSource=admin")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> thingSnapshots = MongoSpark.load(jsc).toDF();
//        thingSnapshots.printSchema();
//        thingSnapshots.show();

        thingSnapshots.createOrReplaceTempView("thingSnapshots");

        Dataset<Row> thingSnapshotsResult = spark.sql("SELECT _id id, value._id _id, value.serialNumber, value.zone.value.name FROM thingSnapshots");
        thingSnapshotsResult.printSchema();
        thingSnapshotsResult.show();
//
//
        Dataset<Row> things = MongoSpark.load(jsc, ReadConfig.create(spark).withOption("collection", "things")).toDF();
        things.createOrReplaceTempView("things");
        Dataset<Row> thingsResult = spark.sql("SELECT _id, serialNumber, zone.time FROM things");
        thingsResult.printSchema();
        thingsResult.show();


//        Dataset<Row> merge = spark.sql("SELECT _id, serialNumber, zone.value.name FROM things");
//
//
        Dataset<Row> merge  = thingsResult.join(thingSnapshotsResult,"_id");
//
        merge.show();
//
//        Dataset<Row> mongoLoadedDF = MongoSpark.load(jsc, readConfig).toDF();

//        // Load data with explicit schema
//        Dataset<Character> explicitDS = MongoSpark.load(jsc).toDS(Character.class);
//        explicitDS.printSchema();
//        explicitDS.show();


//        // Create the temp view and execute the query
//        explicitDS.createOrReplaceTempView("characters");
//        Dataset<Row> thingSnapshots = spark.sql("SELECT name, age FROM characters WHERE age >= 100");
//        thingSnapshots.show();
//
//        // Write the data to the "hundredClub" collection
        MongoSpark.write(merge).option("collection", "mergeResult").mode("overwrite").save();
//
//        // Load the data from the "hundredClub" collection
//        MongoSpark.load(spark, ReadConfig.create(spark).withOption("collection", "hundredClub"), Character.class).show();

        jsc.close();

    }
}