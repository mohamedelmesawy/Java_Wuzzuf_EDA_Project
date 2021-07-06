package com.iti.wuzzufeda.controllers;

import com.iti.wuzzufeda.services.WordCountService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/wordcount")
public class WordCountController {

    @Autowired
    WordCountService service;


//    @GetMapping(value = "/{words}")
//    public Map<String, Long> count(@PathVariable String words) {
//        List<String> wordList = Arrays.asList(words.split("\\|"));
//        return service.getCount(wordList);
//    }

    @GetMapping(value = "/summary")
    public String count() {
        return service.getSummary();
    }




}
