package com.iti.wuzzufeda.controllers;

import com.iti.wuzzufeda.models.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.iti.wuzzufeda.services.EDAService;

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
