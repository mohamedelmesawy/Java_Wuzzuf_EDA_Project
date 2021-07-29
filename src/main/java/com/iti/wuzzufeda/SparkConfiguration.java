package com.iti.wuzzufeda;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfiguration {
    private static SparkSession sparkSession;
    private static String appName = "Wuzzuf_project";
    private static String masterUri = "local[*]";
    // private static String sparkHome = "src/main/resources/hadoop/bin";

   // public  static SparkConf sparkConf() {
   //     SparkConf sparkConf = new SparkConf()
   //             .setAppName(appName)
   //             .setSparkHome(sparkHome)
   //             .setMaster(masterUri)
   //             .set("spark.driver.allowMultipleContexts", "true");
   //
   //     return sparkConf;
   // }
   //
   // public static JavaSparkContext javaSparkContext() {
   //     return new JavaSparkContext(sparkConf());
   // }

    public static SparkSession sparkSession() {
        sparkSession = SparkSession
                .builder()
                .appName(appName)
                .master(masterUri)
                .getOrCreate();

        return sparkSession;
    }

    public static SparkSession getSparkSession(){
        return sparkSession;
    }

}
