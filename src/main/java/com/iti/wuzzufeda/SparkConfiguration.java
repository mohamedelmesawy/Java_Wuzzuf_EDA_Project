package com.iti.wuzzufeda;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfiguration {

    private static String appName = "Wuzzuf_project";

    private static String masterUri = "local[6]";

    private static String sparkHome = "src/main/resources/hadoop/bin";

    private static SparkSession sSession;


//    public  static SparkConf sparkConf() {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName(appName)
//                .setSparkHome(sparkHome)
//                .setMaster(masterUri)
//                .set("spark.driver.allowMultipleContexts", "true");
//
//        return sparkConf;
//    }
//
//    public static JavaSparkContext javaSparkContext() {
//        return new JavaSparkContext(sparkConf());
//    }


    public static SparkSession sparkSession() {
        sSession = SparkSession
                .builder()
                .appName("app")
                .master("local[4]")
                .getOrCreate();

        return sSession;
    }

    public static SparkSession getSparkSession(){

        return sSession;
    }


}
