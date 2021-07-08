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
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.iti.wuzzufeda.services.EDAService;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


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
    public String[] summary(){
        Dataset<Row> result = edaService.getDataset();

        return result.columns();
    }

    @GetMapping(value = "/alljobs")
    public List<Job> alljobs(){
        return edaService.getAllJobs().subList(0, 100);
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
    public Tuple2<String, String>[] test() {

//        return (edaService.getDataset().columns());
        return edaService.getDataset().dtypes();

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


//    .getDataset().toJSON();
//    .map(row -> row.mkString(), Encoders.STRING()).collectAsList();
//   .map(row -> row.getString(0), Encoders.STRING()).collectAsList();
//    .select(struct("*").as("col")).select(to_json(col("col")))


    @GetMapping(value = "/test3")
    public String[] test3() {

        return edaService.testing();
    }





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
