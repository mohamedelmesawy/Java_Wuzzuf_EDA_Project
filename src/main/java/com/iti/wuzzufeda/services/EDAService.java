package com.iti.wuzzufeda.services;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;
import org.apache.spark.sql.Column;
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

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    public Dataset<Row> setDataset(){
        this.dataset = JobsDAO.readCSVUsingSpark(filePath, sparkSession,"%");

        return dataset;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public List<Job> getAllJobs(){
        try {
            return JobsDAO.getListOfJobsFromCSV(filePath, "%");
        } catch (IOException e) {
            return null;
        }
    }

    public void cleanData(){
        // clean data of   < this.dataset >
        this.dataset = PreprocessingHelper.removeNulls(this.dataset);
        this.dataset = PreprocessingHelper.removeDuplicates(this.dataset);
//        this.dataset = PreprocessingHelper.encodeCategory(this.dataset, "_____");
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



    // DELETE MEEEEEEEE TESTING
    public Dataset<Row> testing(){
        return PreprocessingHelper.encodeCategoricalFeatures(this.dataset,List.of("Type"));
    }

}
