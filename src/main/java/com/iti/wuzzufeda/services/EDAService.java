package com.iti.wuzzufeda.services;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;


//@Service
public class EDAService {
    // Business Layer
    private Dataset<Row> dataset;

    public EDAService(String filePath) {
//        Logger.getLogger("org").setLevel(Level.ERROR);
//        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");   // using 3 CPU cores
//        JavaSparkContext context = new JavaSparkContext(conf);


        this.dataset = JobsDAO.readCSVUsingSpark(filePath);
    }


    public void cleanData(){
        // clean data of   < this.dataset >
        this.dataset = PreprocessingHelper.removeNulls(this.dataset);
        this.dataset = PreprocessingHelper.removeDuplicates(this.dataset);
        this.dataset = PreprocessingHelper.encodeCategory(this.dataset, "_____");
    }


    // ------------ SUMMARY ----------------- //
    public String getSummary(){
        return "Summary";
    }

    public String getStructure(){
        return "Structure";
    }


    // ------------ DATA ANALYSIS ----------------- //
    public Map<String, Integer> getMostDemandingCompanies(int count){
        return null;
    }

    public Map<String, Integer> getMostPopularJobs(int count){
        return null;
    }

    public Map<String, Integer> getMostPopularAreas(int count){
        return null;
    }

    public Map<String, Integer> getMostRequiredSkills(int count){
        return null;
    }


    // ------------ MACHINE LEARNING MODELS ----------------- //
    public String getRegressionModel(){
        return "Regression Model";
    }

    public String getKMeansModel(){
        return "KMeans Model";
    }

}
