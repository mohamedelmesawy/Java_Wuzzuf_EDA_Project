package com.iti.wuzzufeda.controllers;

import com.iti.wuzzufeda.models.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.iti.wuzzufeda.services.EDAService;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;


@CrossOrigin
@RestController
@RequestMapping(value = "/api/v1")
public class RESTController {
    @Autowired
    private EDAService edaService;


    @GetMapping(value = {"", "/"})
    public String hello(){
        return "Hello from other side , it's me!!!!!!!!!!!!!!!!!!!!!";
    }

    @GetMapping(value = "/regression")
    public String regression(){
        return edaService.getRegressionModel();
    }

    @GetMapping(value = "/kmeans")
    public String kmeans(){
        return edaService.getKMeansModel();
    }

    @GetMapping(value = "/summary")
    public Map<String, Object> summary(){

        return edaService.getSummary();
    }

    @GetMapping(value = "/structure")
    public Tuple2<String, String>[] structure(){
        Tuple2<String, String>[] edaServiceStructure = edaService.getStructure();

        return edaServiceStructure;
    }


    @GetMapping(value = "/mostDemandingCompanies")
    public Map<String, Long> mostDemandingCompanies(){
        Map<String, Long> result = this.edaService.getMostDemandingCompanies();

        return result;
    }


    @GetMapping(value = "/mostDemandingCompanies/{count}")
    public Map<String, Long> mostDemandingCompanies(@PathVariable int count){
        Map<String, Long> result = this.edaService.getMostDemandingCompanies(count);

        return result;
    }










    @GetMapping(value = "/alljobs")
    public List<Job> alljobs(){
        return edaService.getAllJobs().subList(0, 100);
    }

    @GetMapping(value = "/allJobsSpark")
    public List<Map<String, String>> allJobsSpark() {
        return edaService.getListOfJobsFromDataSet();
    }

    @GetMapping(value = "/image", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> image() throws IOException {
        final ByteArrayResource inputStream =
                new ByteArrayResource(
                        Files.readAllBytes(
                                Paths.get("src/main/resources/static/developers.jpeg")));

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);

    }




//////////////////////////////////Remove Test/////////////////////////////////////////
    @GetMapping(value = "/test")
    public Map<String, Long> test() {

//        return edaService.getDataset().as(Encoders.bean(Job.class)).;

        return edaService.getMostDemandingCompanies(10);
    }

    @GetMapping(value = "/test1")
    public List<String> test1() {
        Dataset<String> d = edaService.getDataset().toJSON();

        return  d.collectAsList();

    }

    @GetMapping(value = "/test2")
    public Object test2() {
        Object d = edaService.getDataset().toJSON().head(1);

        return  d;

    }


    @GetMapping(value = "/test3")
    public long test3() {

        return edaService.getDataset().count();
    }




//    .getDataset().toJSON();
//    .map(row -> row.mkString(), Encoders.STRING()).collectAsList();
//   .map(row -> row.getString(0), Encoders.STRING()).collectAsList();
//    .select(struct("*").as("col")).select(to_json(col("col")))





    // PRIVATE Functions, Delete me!!!!!!!!!!!!!
//    @GetMapping(value = "/{name}")
//    public String greetingWithName(@PathVariable String name){
//        return "Hello, welcome to EDA " + name;
//    }

//    @GetMapping(value = "/jobs")
//    public List<Job> jobs(){
//        return null;
//    }

//    @GetMapping(value = "/jobs/{count}")
//    public List<Job> jobsMost(@PathVariable int count){
//        return null;
//    }


}
