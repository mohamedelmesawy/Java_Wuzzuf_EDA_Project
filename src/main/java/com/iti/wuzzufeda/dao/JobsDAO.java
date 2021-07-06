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
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;


public class JobsDAO {

    public static Dataset<Row> readCSVUsingSpark(String filePath, SparkSession sparkSession, String delimiter){
        // Creating data-frame reader
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");
        dataFrameReader.option("sep", delimiter);

        // Read data
        Dataset<Row> dataset = dataFrameReader.csv(filePath);

        return dataset;
    }

    public static List<Job> getListOfJobsFromCSV(String filePath, String delimiter) throws IOException {
        List<Job> jobs = new ArrayList<>();
        File file = new File(filePath);

        // Create list of lines
        List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("ISO-8859-1"));

        for (int index =1 ; index < lines.size(); index++){
            String line = lines.get(index);
            String[] attributes = line.split(delimiter);

            for (int i = 0; i < attributes.length; i++)
                attributes[i] = attributes[i].trim();

            try {
                jobs.add(new Job(
                            attributes[1],
                            attributes[2],
                            attributes[3],
                            encodeJobType(attributes[4]),
                            encodeJobLevel(attributes[5]),
                            attributes[6],
                            attributes[7],
                        List.of(attributes[8].split(","))

                ));
            }catch (Exception ignored){ }
        }

        return jobs;
    }

    public static JobLevel encodeJobLevel(String jobLevel){
        if (jobLevel.equals("Entry Level")) return JobLevel.ENTRY_LEVEL;
        else if (jobLevel.equals("Experienced")) return JobLevel.EXPERIENCED;
        else if (jobLevel.equals("Freelance / Project")) return JobLevel.FREELANCE_PROJECT;
        else if (jobLevel.equals("Manager")) return JobLevel.MANAGER;
        else if (jobLevel.equals("Part Time")) return JobLevel.PART_TIME;
        else if (jobLevel.equals("Senior Management")) return JobLevel.SENIOR_MANAGEMENT;
        else if (jobLevel.equals("Shift Based")) return JobLevel.SHIFT_BASED;
        else if (jobLevel.equals("Student")) return JobLevel.STUDENT;
        else if (jobLevel.equals("Work From Home")) return JobLevel.WORK_FROM_HOME;
        else return null;
    }

    public static JobType encodeJobType(String jobType){
        if (jobType.equals("Freelance / Project")) return JobType.FREELANCE_PROJECT;
        else if (jobType.equals("Full Time")) return JobType.FULL_TIME;
        else if (jobType.equals("Internship")) return JobType.INTERNSHIP;
        else if (jobType.equals("Part Time")) return JobType.PART_TIME;
        else if (jobType.equals("Shift Based")) return JobType.SHIFT_BASED;
        else if (jobType.equals("Work From Home")) return JobType.WORK_FROM_HOME;
        else return null;
    }

}
