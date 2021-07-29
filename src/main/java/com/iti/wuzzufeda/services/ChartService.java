package com.iti.wuzzufeda.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;
import org.knowm.xchart.style.AxesChartStyler;
import org.knowm.xchart.style.Styler;


import java.awt.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChartService {

    // -------------------------------------- PIE CHART ----------------------------------------- //
    public static void getJobsPieChart(Map<String,Long> data, String title, int height, int width,
                                       String savingName ) throws IOException {

        // Create Chart
        PieChart chart = new PieChartBuilder()
                .width(width)
                .height(height)
                .title(title)
                .theme(Styler.ChartTheme.GGPlot2)
                .build();

        // Customize Chart
        Color[] sliceColors = new Color[] {
                new Color(224, 68, 14),
                new Color(230, 105, 62),
                new Color(236, 143, 110),
                new Color(243, 180, 159),
                new Color(246, 199, 182) };

        chart.getStyler().setSeriesColors(sliceColors);

        // Series
        int i = 0;
        for (Map.Entry entry : data.entrySet() ) {
            if(i == 5) break;
            chart.addSeries(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            i++;
        }
        BitmapEncoder.saveBitmapWithDPI(chart, savingName, BitmapEncoder.BitmapFormat.PNG, 300);
    }


    // -------------------------------------- BAR CHART ----------------------------------------- //
    public static void  getBarChart(Map<String,Long> data, String title,
                                    String xlabel, String ylabel, int count, int height, int width,
                                    String savingName) throws IOException {
        // Create Chart
        CategoryChart chart = new CategoryChartBuilder()
                .width(width)
                .height(height)
                .title(title)
                .xAxisTitle(xlabel)
                .yAxisTitle(ylabel)
                .theme(Styler.ChartTheme.GGPlot2)
                .build();

        // Customize Chart
        // chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setXAxisLabelRotation(90);
        chart.getStyler().setAnnotationsFont(new Font(Font.MONOSPACED, Font.BOLD, 14));

        List<String> keys = data.keySet()
                            .stream()
                            .limit(count)
                            .collect(Collectors.toList());

        List<Long> values = data.values()
                .stream()
                .limit(count)
                .collect(Collectors.toList());

        // Series
        chart.addSeries("Bar Chart", keys, values);
        BitmapEncoder.saveBitmapWithDPI(chart, savingName, BitmapEncoder.BitmapFormat.PNG, 300);
    }

    // ------------------------------- SCATTER CHART for KMeans --------------------------------- //
    public static void graphScatterPlotKmeans(Dataset<Row> prediction, int k, String title, int height,
                                   int width, String xlabel, String ylabel,
                                   String savingName) throws IOException{
        Map<List<Double>, List<Double>> map = new HashMap<>();

        for (int i = 0; i < k; i++) {
            List<Double> x = prediction.filter("Predicted ='" + i +"'").select("Title_indexed")
                    .collectAsList().stream().map(r -> Double.valueOf(r.mkString())).collect(Collectors.toList());
            List<Double> y = prediction.filter("Predicted ='" + i +"'").select("Company_indexed")
                    .collectAsList().stream().map(r -> Double.valueOf(r.mkString())).collect(Collectors.toList());

            map.put(x, y);
        }

        // Create Chart
        XYChart chart = new XYChartBuilder()
                .width(width)
                .height(height)
                .title(title)
                .xAxisTitle(xlabel)
                .yAxisTitle(ylabel)
                .theme(Styler.ChartTheme.GGPlot2)
                .build();

        // Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setXAxisLabelRotation(90);
        chart.getStyler().setAnnotationsFont(new Font(Font.MONOSPACED, Font.BOLD, 14));

        // Series
        int counter = 1;
        for (Map.Entry<List<Double>, List<Double>> set : map.entrySet()) {
            chart.addSeries(String.valueOf(counter), set.getKey(), set.getValue());

            counter++;
        }

        // Show it
        // new SwingWrapper(chart).displayChart();

        BitmapEncoder.saveBitmapWithDPI(chart, savingName, BitmapEncoder.BitmapFormat.PNG, 300);
    }
}
