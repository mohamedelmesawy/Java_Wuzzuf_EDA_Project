package com.iti.wuzzufeda.controllers;

import com.iti.wuzzufeda.models.Job;
import com.iti.wuzzufeda.services.EDAService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/api/v1")
public class RESTController {
//    @Autowired
    private String filePath = "src/main/resources/Wuzzuf_Jobs.csv";
    private EDAService edaService = new EDAService(filePath);

    @GetMapping(value = {"", "/"})
    public String hello(){

        return "Hello, it's me!!!!!!!!!!!!!!!!!!!!!";
    }


    @GetMapping(value = "/regression")
    public String regression(){
        return edaService.getRegressionModel();
    }

    @GetMapping(value = "/kmeans")
    public String kmeans(){
        return edaService.getKMeansModel();
    }







    // PRIVATE Functions, Delete me!!!!!!!!!!!!!
//    @GetMapping(value = "/{name}")
//    public String greetingWithName(@PathVariable String name){
//        return "Hello, welcome to EDA " + name;
//    }
//
//    @GetMapping(value = "/jobs")
//    public List<Job> jobs(){
//        return null;
//    }
//
//    @GetMapping(value = "/jobs/{count}")
//    public List<Job> jobsMost(@PathVariable int count){
//        return null;
//    }


}
