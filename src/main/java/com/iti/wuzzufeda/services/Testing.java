package com.iti.wuzzufeda.services;

import com.iti.wuzzufeda.dao.JobsDAO;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class Testing {
    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.ERROR);
//        Logger.getRootLogger().setLevel(Level.OFF);

        Logger.getRootLogger().setLevel(Level.ERROR);
        String filePath = "src/main/resources/Wuzzuf_Jobs.csv";

        // Creating spark session
//        SparkConf conf = new SparkConf().setAppName("Wuzzuf").setMaster("local[*]");
        // cache sizes

//        SparkSession sparkSession = SparkSession.builder().appName("Wuzzuf").master("local[*]").getOrCreate();
//
//        Dataset<Row> dataset = JobsDAO.readCSVUsingSpark(filePath, sparkSession);
//
//        dataset.printSchema();
//
//        dataset = dataset.select("Title");
//        dataset.show(10);






    }


}
