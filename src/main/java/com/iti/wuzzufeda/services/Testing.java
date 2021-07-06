package com.iti.wuzzufeda.services;

import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;


public class Testing {
    public static void main(String[] args) {



        String filePath = "src/main/resources/Wuzzuf_Jobs_delimiter.csv";

        try {
            List<Job> jobs = JobsDAO.getListOfJobsFromCSV(filePath, "%");
            jobs.forEach(System.out::println);

            System.out.println(jobs.size());
        } catch (IOException e) {
            e.printStackTrace();
        }










    }


}
