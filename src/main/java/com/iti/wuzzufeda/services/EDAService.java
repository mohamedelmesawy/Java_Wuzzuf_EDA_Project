package com.iti.wuzzufeda.services;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.iti.wuzzufeda.SparkConfiguration;
import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;


import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;


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


    @Autowired
    public void cleanData() {
        this.dataset = PreprocessingHelper.removeNulls(this.dataset);
        this.dataset = PreprocessingHelper.removeDuplicates(this.dataset);
        this.dataset = PreprocessingHelper.encodeCategoricalFeatures(this.dataset, Arrays.asList("Type", "Level"));
    }


    // ---------------------------------- SUMMARY ---------------------------------- //
    public Map<String, Object> getSummary() {
        Map<String, Object> map = new HashMap<>();

        map.put("Row count", this.dataset.count());
        map.put("Features count", this.dataset.columns().length);
        map.put("Features List", this.dataset.columns());
        map.put("Distinct Job Levels", this.dataset.select("Level").distinct().as(Encoders.STRING()).collectAsList());
        map.put("Distinct Job Types", this.dataset.select("Type").distinct().as(Encoders.STRING()).collectAsList());

        return map;
    }

    public Tuple2<String, String>[] getStructure() {
       return this.dataset.dtypes();
    }


    // ------------------------ DATA ANALYSIS ---------------------------------- //
    public Map<String, Long> getMostDemandingCompanies(int count) {
        Map<Row, Long> result = this.dataset
                .select("Company")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(result, count);
    }

    public Map<String, Long> getMostDemandingCompanies() {
        Map<Row, Long> result = this.dataset
                .select("Company")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(result);
    }

    public Map<String, Long> getMostPopularJobs(int count) {
        Map<Row, Long> jobsCount = dataset
                .select("Title")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(jobsCount, count);
    }

    public Map<String, Long> getMostPopularJobs() {
        Map<Row, Long> jobsCount = dataset
                .select("Title")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(jobsCount);

    }

    public Map<String, Long> getMostPopularAreas(int count) {
        Map<Row, Long> result = this.dataset
                .select("Location")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(result, count);
    }

    public Map<String, Long> getMostPopularAreas() {
        Map<Row, Long> result = this.dataset
                .select("Location")
                .javaRDD()
                .countByValue();

        return PreprocessingHelper.sortMap(result);
    }

    public Map<String, Long> getMostRequiredSkills(int count) {
        Map<String, Long> data = dataset.select("Skills")
                .javaRDD()
                .map(row -> row.mkString().split(","))
                .flatMap(array -> Arrays.stream(array).iterator())
                .countByValue()
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(Map.Entry::getValue)))
                .map(entry -> new AbstractMap.SimpleEntry<String, Long>(entry.getKey(), entry.getValue()))
                .limit(count)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                );
        return data;
    }

    public Map<String, Long> getMostRequiredSkills() {
        Map<String, Long> data = dataset.select("Skills")
                .javaRDD()
                .map(row -> row.mkString().split(","))
                .flatMap(array -> Arrays.stream(array).iterator())
                .countByValue()
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(Map.Entry::getValue)))
                .map(entry -> new AbstractMap.SimpleEntry<String, Long>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                );
        return data;
    }


    // ---------------------------- MACHINE LEARNING MODELS -------------------------- //
    public String getRegressionModel() {
        return "Regression Model";
    }

    public String getKMeansModel() {
        return "KMeans Model";
    }


    // ---------------------------- Retrieve Data from CSV File -------------------------- //
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

    public List<Map<String, String>> getListOfJobsFromDataSet(){

        List<Map<String, String>> data =  dataset.toJSON()
                .collectAsList()
                .stream()
                .map(value -> {
                    value = value.substring(1, value.length()-1);           //remove curly brackets
                    String[] keyValuePairs = value.split("\",\"");     //split the string to creat key-value pairs  ","
                    Map<String, String> map = new HashMap<>();

                    for(String pair : keyValuePairs)                         //iterate over the pairs
                    {
                        String[] entry = pair.split(":");              //split the pairs to get key and value
                        map.put(entry[0].trim().replaceAll("^\"+|\"+$", ""),
                                entry[1].trim().replaceAll("^\"+|\"+$", "")
                        );                                                   //add them to the hashmap and trim whitespaces

                    }

                    return map;
                }).collect(Collectors.toList());

        return data;
    }

}
