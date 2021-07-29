package com.iti.wuzzufeda;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WuzzufEDAApplication {

    public static void main(String[] args) {
        SparkConfiguration.sparkSession();
        SpringApplication.run(WuzzufEDAApplication.class, args);
    }

}
