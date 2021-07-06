package com.iti.wuzzufeda.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PreprocessingHelper {

    public static Dataset<Row> removeNulls(Dataset<Row> dataset){
        dataset = dataset.na().drop();
        return dataset;
    }

    public static Dataset<Row> removeDuplicates(Dataset<Row> dataset){
        return null;
    }

    public static Dataset<Row> encodeCategory(Dataset<Row> dataset, String columnName){
        return null;
    }

}
