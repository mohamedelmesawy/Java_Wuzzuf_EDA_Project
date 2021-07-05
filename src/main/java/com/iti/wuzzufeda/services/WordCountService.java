package com.iti.wuzzufeda.services;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class WordCountService {

    @Autowired
    JavaSparkContext sc;


    public Map<String, Long> getCount(List<String> wordList) {
        JavaRDD<String> words = sc.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();

//        SparkSession sparkSession = SparkSession.builder()
//                .appName("Wuzzuf")
//                .master("local[*]")
//                .getOrCreate();

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");
        SparkSession sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();

        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");

//        // Read data
        Dataset<Row> dataset = dataFrameReader.csv( "src/main/resources/Wuzzuf_Jobs.csv");

        return Map.of("asss", (long) dataset.columns().length);
//        return wordCounts;
    }


    public String getSummary(){
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");
        SparkSession sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();

        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");

//        // Read data
        Dataset<Row> dataset = dataFrameReader.csv( "src/main/resources/Wuzzuf_Jobs.csv");

        String ss = "";
        for (String col :dataset.columns()) {
            ss += col + " - ";
        }

        return ss;
    }

}