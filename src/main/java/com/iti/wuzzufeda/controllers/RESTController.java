package com.iti.wuzzufeda.controllers;

import com.iti.wuzzufeda.models.Job;
import com.iti.wuzzufeda.services.ChartService;
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
import java.util.Arrays;
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
        return "Hello from the other side , it's me!!!!!!!!!!!!!!!!!!!!!";
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

    @GetMapping(value = "/mostPopularJobs")
    public Map<String, Long> mostPopularJobs() {
        return  edaService.getMostPopularJobs();
    }

    @GetMapping(path = "/mostPopularJobs/{count}")
    public Map<String, Long> mostPopularJobs(@PathVariable int count) {
        return  edaService.getMostPopularJobs(count);
    }

    @GetMapping(value = "/mostPopularAreas")
    public Map<String, Long> mostPopularAreas() {
        return  edaService.getMostPopularAreas();
    }

    @GetMapping(path = "/mostPopularAreas/{count}")
    public Map<String, Long> mostPopularAreas(@PathVariable int count) {
        return  edaService.getMostPopularAreas(count);
    }

    @GetMapping(value = "/mostRequiredSkills")
    public Map<String, Long> mostRequiredSkills() {
        return  edaService.getMostRequiredSkills();
    }

    @GetMapping(path = "/mostRequiredSkills/{count}")
    public Map<String, Long> mostRequiredSkills(@PathVariable int count) {
        return  edaService.getMostRequiredSkills(count);
    }

    @GetMapping(value = "/alljobs")
    public List<Job> alljobs(){
        return edaService.getAllJobs().subList(0, 100);
    }

    @GetMapping(value = "/allJobsSpark")
    public List<Map<String, Object>> allJobsSpark() throws IOException {
        return edaService.getListOfJobsFromDataSet();
    }

    @GetMapping(value = "/yearsofexp")
    public List<Map<String, Object>> getFactorizedYearsColumn() throws IOException {
        List<Map<String, Object>> jobsFromDataSet = edaService.getListOfJobsFromDataSet();

        return edaService.factorizeYearsColumn(jobsFromDataSet);
    }

    @GetMapping(value = "/image/companiespiechart", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> companiespiechart() throws IOException {

        String filename = "src/main/resources/static/companies.png";

        ChartService.getJobsPieChart(edaService.getMostDemandingCompanies(5),
                "Highest 5 Demanding Companies",200,400,filename);

        final ByteArrayResource inputStream =
                new ByteArrayResource(
                        Files.readAllBytes(
                                Paths.get(filename)));

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    @GetMapping(value = "/image/jobsbarchart", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> jobsbarchart() throws IOException {

        String filename = "src/main/resources/static/jobs.png";

        ChartService.getBarChart(edaService.getMostPopularJobs(10),
                "Most Popular 10 Jobs","Job Title","Frequency",
                10,600,800,filename);

        final ByteArrayResource inputStream =
                new ByteArrayResource(
                        Files.readAllBytes(
                                Paths.get(filename)));

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    @GetMapping(value = "/image/areasbarchart", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<Resource> areasbarchart() throws IOException {

        String filename = "src/main/resources/static/areasbarchart.png";

        ChartService.getBarChart(edaService.getMostPopularAreas(10),
                "Most Popular Areas","Area","Frequency",
                10,600,800,filename);

        final ByteArrayResource inputStream =
                new ByteArrayResource(
                        Files.readAllBytes(
                                Paths.get(filename)));

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    @GetMapping(value = "/image/kmeans/{k}", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<ByteArrayResource> kmeans(@PathVariable int k) throws IOException {
        String filename = "src/main/resources/static/kmeans.png";
        Dataset<Row> prediction = edaService.getKMeansModel(edaService.getDataset(), Arrays.asList("Title", "Company"), k);

        ChartService.graphScatterPlotKmeans(prediction, k, "Title vs. Company", 600, 800, "Title_Index", "Company_Index", filename);

        final ByteArrayResource inputStream =
                new ByteArrayResource(
                        Files.readAllBytes(
                                Paths.get(filename)));

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

}
