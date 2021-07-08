package com.iti.wuzzufeda.models;

import java.io.Serializable;
import java.util.List;

public class Job implements Serializable {
    private String title;
    private String company;
    private String location;
    private JobType type;
    private JobLevel level;
    private String yearsOfExperience;
    private String country;
    private List<String> skills;


    public Job(String title, String company, String location, JobType type, JobLevel level, String yearsOfExperience, String country, List<String> skills) {
        this.title = title;
        this.company = company;
        this.location = location;
        this.type = type;
        this.level = level;
        this.yearsOfExperience = yearsOfExperience;
        this.country = country;
        this.skills = skills;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public JobType getType() {
        return type;
    }

    public void setType(JobType type) {
        this.type = type;
    }

    public JobLevel getLevel() {
        return level;
    }

    public void setLevel(JobLevel level) {
        this.level = level;
    }

    public String getYearsOfExperience() {
        return yearsOfExperience;
    }

    public void setYearsOfExperience(String yearsOfExperience) {
        this.yearsOfExperience = yearsOfExperience;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public List<String> getSkills() {
        return skills;
    }

    public void setSkills(List<String> skills) {
        this.skills = skills;
    }

    @Override
    public String toString() {
        return "Job{" +
                "title='" + title + '\'' +
                ", company='" + company + '\'' +
                ", location='" + location + '\'' +
                ", type=" + type +
                ", level=" + level +
                ", yearsOfExperience='" + yearsOfExperience + '\'' +
                ", country='" + country + '\'' +
                ", skills= [ " + String.join(", ", skills)  + " ] " +
                '}';
    }

}
