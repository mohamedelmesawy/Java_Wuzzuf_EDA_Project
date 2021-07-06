package com.iti.wuzzufeda.services;

import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Testing {
    public static void main(String[] args) throws IOException {

        String filePath = "src/main/resources/Wuzzuf_Jobs.csv";

        List<Job> jobs = new ArrayList<>();

        File file = new File(filePath);
        // Create list of lines
        List<String> lines = Files.readAllLines(file.toPath());
        System.out.println(lines);

    }
}
