package com.iti.wuzzufeda.services;

import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.stream.Collectors;

public class PreprocessingHelper {

    public static Dataset<Row> removeNulls(Dataset<Row> dataset){
        dataset = dataset.na().drop();
        return dataset;
    }

    public static Dataset<Row> removeDuplicates(Dataset<Row> dataset){
        dataset = dataset.dropDuplicates();
        return dataset;
    }

    public static Dataset<Row> encodeCategoricalFeatures(Dataset<Row> dataset, List<String> features ){
        Dataset<Row> newDataset = null;

        for (String feature: features){
            StringIndexerModel indexer = new StringIndexer()
                    .setInputCol(feature)
                    .setOutputCol(feature + "_indexed")
                    .fit(dataset);

            newDataset = indexer.transform(dataset);

            OneHotEncoder encoder = new OneHotEncoder()
                    .setInputCol(feature + "_indexed")
                    .setOutputCol(feature + "_vec")
                    .setDropLast(true);

            newDataset = encoder.transform(newDataset);
        }
        return newDataset;

    }

    public static Map<String, Long> sortMap(Map<Row, Long> data){
        Map<String, Long> sordedData = data.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(Map.Entry::getValue)))
                .map(entry -> new AbstractMap.SimpleEntry<String, Long>(entry.getKey().mkString(), entry.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                );

        return sordedData;
    }

    public static Map<String, Long> sortMap(Map<Row,Long> data, int count){
        Map<String, Long> sordedData = data.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(Map.Entry::getValue)))
                .map(entry -> new AbstractMap.SimpleEntry<String, Long>(entry.getKey().mkString(), entry.getValue()))
                .limit(count)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                );

        return sordedData;
    }


}
