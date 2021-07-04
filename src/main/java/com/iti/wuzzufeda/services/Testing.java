package com.iti.wuzzufeda.services;

import com.iti.wuzzufeda.dao.JobsDAO;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class Testing {
    public static void main(String[] args) {

        String filePath = "src/main/resources/Wuzzuf_Jobs.csv";

//        Logger.getLogger("org").setLevel(Level.ERROR);
//        Logger.getRootLogger().setLevel(Level.OFF);
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }

        // Creating spark session
        SparkSession sparkSession = SparkSession.builder().appName("Wuzzuf").master("local[*]").getOrCreate();

        Dataset<Row> dataset = JobsDAO.readCSVUsingSpark(filePath, sparkSession);

        dataset.printSchema();
//        dataset.show();

    }


}
