package com.iti.wuzzufeda.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.iti.wuzzufeda.SparkConfiguration;
import com.iti.wuzzufeda.WuzzufEDAApplication;
import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class EDAService {
    // Business Layer
    @Value("${filepath}")
    private String filePath;

    private Dataset<Row> dataset = null;

    private SparkSession sparkSession = SparkConfiguration.getSparkSession();


    @Autowired
    public Dataset<Row> setDataset() {
        this.dataset = JobsDAO.readCSVUsingSpark(filePath, sparkSession, "%");

        return dataset;
    }


    public Dataset<Row> getDataset() {
        return dataset;
    }

    public List<Job> getAllJobs() {
        try {
            return JobsDAO.getListOfJobsFromCSV(filePath, "%");
        } catch (IOException e) {
            return null;
        }
    }

    public void cleanData() {
        // clean data of   < this.dataset >
        this.dataset = PreprocessingHelper.removeNulls(this.dataset);
        this.dataset = PreprocessingHelper.removeDuplicates(this.dataset);
//        this.dataset = PreprocessingHelper.encodeCategory(this.dataset, "_____");
    }


    public List<Map<String, String>> getListOfJobsFromDataSet(){

        List<Map<String, String>> data =  dataset.toJSON().collectAsList().stream().map(value -> {
            value = value.substring(1, value.length()-1);           //remove curly brackets
            String[] keyValuePairs = value.split("\",\"");              //split the string to creat key-value pairs
            Map<String,String> map = new HashMap<>();

            for(String pair : keyValuePairs)                        //iterate over the pairs
            {
                String[] entry = pair.split(":");                   //split the pairs to get key and value
                map.put(entry[0].trim().replaceAll("^\"+|\"+$", ""),
                        entry[1].trim().replaceAll("^\"+|\"+$", ""));          //add them to the hashmap and trim whitespaces

            }
            return map;
        }).collect(Collectors.toList());

        return data;
    }



    // ------------ SUMMARY ----------------- //
    public String getSummary() {
        return "Summary";
    }

    public String getStructure() {
        return "Structure";
    }


    // ------------ DATA ANALYSIS ----------------- //
    public Map<String, Integer> getMostDemandingCompanies(int count) {
        return null;
    }

    public Map<String, Integer> getMostPopularJobs(int count) {
        return null;
    }

    public Map<String, Integer> getMostPopularAreas(int count) {
        return null;
    }

    public Map<String, Integer> getMostRequiredSkills(int count) {
        return null;
    }


    // ------------ MACHINE LEARNING MODELS ----------------- //
    public String getRegressionModel() {
        return "Regression Model";
    }

    public String getKMeansModel() {
        return "KMeans Model";
    }





    //////////////////// DELETE MEEEEEEEE TESTING /////////////////////////////
    public String[] testing() {
        List l1 = new ArrayList(); l1.add("Type");
        return PreprocessingHelper.encodeCategoricalFeatures(this.dataset, l1).columns();
    }

//    public String testing2() {
//
//        var col = dataset.select("Type").
//                collectAsList().stream().map(e -> e.toString()).collect(Collectors.toList());
//
//        var colJson = new Gson().toJson(col);
//
//        return colJson;
//
//    }

//    public String[] testing2() {
//
//         List<String> col = dataset.select("Type").
//                collectAsList().stream().map(e -> e.toString()).collect(Collectors.toList());
//
//        String[] colArray = col.toArray(new String[0]);
//
//        return colArray;
//
//    }
}
