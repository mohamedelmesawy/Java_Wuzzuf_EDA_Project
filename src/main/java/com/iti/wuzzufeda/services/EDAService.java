package com.iti.wuzzufeda.services;

import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import com.iti.wuzzufeda.SparkConfiguration;
import com.iti.wuzzufeda.dao.JobsDAO;
import com.iti.wuzzufeda.models.Job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
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
        return JobsDAO.readCSVUsingSpark(filePath, sparkSession, "%");
    }

    @Autowired
    public void cleanData() {
        this.dataset = PreprocessingHelper.removeNulls(this.dataset);
        this.dataset = PreprocessingHelper.removeDuplicates(this.dataset);
     // this.dataset = PreprocessingHelper.encodeCategoricalFeatures(this.dataset, Arrays.asList("Type", "Level"));
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
    public Dataset<Row> getKMeansModel(Dataset<Row> dataset, List<String> features, int k) {
        // Feature Encoding
        dataset = PreprocessingHelper.encodeCategoricalFeatures(dataset, features);

        // Create the VectorAssembler which contains the features
        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[] { "Title_indexed", "Company_indexed" });
        vectorAssembler.setOutputCol("features");

        // Transform the trained Dataset
        Dataset<Row> transformedDataset = vectorAssembler.transform(dataset.na().drop());

        // Train the KMeans Model
        KMeans kmeans = new KMeans().setK(k).setSeed(1L);
        kmeans.setFeaturesCol("features");
        kmeans.setPredictionCol("Predicted");

        // Clustering using KMeans
        KMeansModel model = kmeans.fit(transformedDataset);

        // Predict
        Dataset<Row> prediction = model.transform(transformedDataset);

        return prediction;
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

    public List<Map<String, Object>> getListOfJobsFromDataSet() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> data = new ArrayList<>();
        List<String> stringDataset = this.dataset.toJSON().collectAsList();

        for (String s : stringDataset) {
            Map<String, Object> map = mapper.readValue(s, new TypeReference<Map<String, Object>>(){ });
            data.add(map);
        }

        return data;
    }


    // ---------------------------- Data-Preprocessing -------------------------- //
    public List<Map<String, Object>> factorizeYearsColumn(List<Map<String, Object>> data){
        List<Map<String, Object>> factorizedDataset = data
                .stream()
                .map(row -> {
                        int yearsOfExp;
                        String txt = row.get("YearsExp").toString().replaceAll("[^\\-0123456789]","");

                        if(txt.isEmpty())
                            yearsOfExp = 0;
                        else if (txt.contains("-")){
                            String[] nums = txt.split("-");
                            yearsOfExp =  ( Integer.parseInt(nums[0]) + Integer.parseInt(nums[1]) ) / 2;
                        }
                        else
                            yearsOfExp = Integer.parseInt(txt);

                        row.remove("Skills");
                        row.remove("Level");
                        row.remove("Type");
                        row.remove("Location");
                        row.remove("Country");
                     // row.remove("Company");
                     // row.remove("Title");
                        row.remove("_c0");

                        row.put("YearsExpFactorized", yearsOfExp);

                        return row;
                    }
                ).collect(Collectors.toList());

        // To convert JSON to DataFrame using List<Row> & Scheme ------------------------------------------
        // List<String> cols = new ArrayList(FactorizedDataSet.get(0).keySet());

        // List<Row> rows = FactorizedDataSet
        //         .stream()
        //         .map(row -> cols.stream().map(c -> (Object) row.get(c).toString()))
        //         .map(row -> row.collect(Collectors.toList()))
        //         .map(row -> JavaConverters.asScalaBufferConverter(row).asScala().toSeq())
        //         .map(Row$.MODULE$::fromSeq)
        //         .collect(Collectors.toList());

        // StructType schema = new StructType(
        //         cols.stream()
        //                 .map(c -> new StructField(c, DataTypes.StringType, true, Metadata.empty()))
        //                 .collect(Collectors.toList())
        //                 .toArray(new StructField[0])
        // );

        // Dataset<Row> NewData = sparkSession.createDataFrame(rows, schema);

        return factorizedDataset;
    }

}
