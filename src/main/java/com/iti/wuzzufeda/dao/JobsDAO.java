package com.iti.wuzzufeda.dao;

import com.iti.wuzzufeda.models.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class JobsDAO {
    public static Dataset<Row> readCSVUsingSpark(String filePath){
        return null;
    }

    public static List<Job> getListOfJobsFromCSV(String filePath){
        return null;
    }

}
