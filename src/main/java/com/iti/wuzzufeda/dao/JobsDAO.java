package com.iti.wuzzufeda.dao;

import com.iti.wuzzufeda.models.Job;
import com.iti.wuzzufeda.models.JobLevel;
import com.iti.wuzzufeda.models.JobType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class JobsDAO {
    public static Dataset<Row> readCSVUsingSpark(String filePath, SparkSession sparkSession){


        // Creating dataframe reader
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");
        // Read data
        Dataset<Row> dataset = dataFrameReader.csv(filePath);

        return dataset;
    }

    public static List<Job> getListOfJobsFromCSV(String filePath) throws IOException {

        List<Job> jobs = new ArrayList<>();

        File file = new File(filePath);
        // Create list of lines
        List<String> lines = Files.readAllLines(file.toPath());
        for (int index =1 ; index < lines.size(); index++){

            String line = lines.get(index);
            String[] attributes = line.split(",");
            for (String attribute : attributes){
                attribute.trim();
            }
            try {
                jobs.add(new Job(
                        attributes[0],
                        attributes[1],
                        attributes[2],
                        encodeJobType(attributes[3]),
                        encodeJobLevel(attributes[4]),
                        attributes[5],
                        attributes[6],
                        List.of(attributes[7])

                ));
            }catch (Exception e){
                e.printStackTrace();
            }

        }



        return null;
    }

    public static JobLevel encodeJobLevel(String jobLevel){
        if (jobLevel == "Entry Level") return JobLevel.ENTRY_LEVEL;
        else if (jobLevel == "Experienced") return JobLevel.EXPERIENCED;
        else if (jobLevel == "Freelance / Project") return JobLevel.FREELANCE_PROJECT;
        else if (jobLevel == "Manager") return JobLevel.MANAGER;
        else if (jobLevel == "Part Time") return JobLevel.PART_TIME;
        else if (jobLevel == "Senior Management") return JobLevel.SENIOR_MANAGEMENT;
        else if (jobLevel == "Shift Based") return JobLevel.SHIFT_BASED;
        else if (jobLevel == "Student") return JobLevel.STUDENT;
        else if (jobLevel == "Work From Home") return JobLevel.WORK_FROM_HOME;
        else return null;
    }

    public static JobType encodeJobType(String jobType){
        if (jobType == "Freelance / Project") return JobType.FREELANCE_PROJECT;
        else if (jobType == "Full Time") return JobType.FULL_TIME;
        else if (jobType == "Internship") return JobType.INTERNSHIP;
        else if (jobType == "Part Time") return JobType.PART_TIME;
        else if (jobType == "Shift Based") return JobType.SHIFT_BASED;
        else if (jobType == "Work From Home") return JobType.WORK_FROM_HOME;
        else return null;
    }


}
