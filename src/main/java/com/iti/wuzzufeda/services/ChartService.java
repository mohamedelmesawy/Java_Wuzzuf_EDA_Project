package com.iti.wuzzufeda.services;

import org.knowm.xchart.*;
import org.knowm.xchart.style.AxesChartStyler;
import org.knowm.xchart.style.Styler;


import java.awt.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChartService {


    // ------------------------------- PIE CHART ---------------------------------- //

    public static void getJobsPieChart(Map<String,Long> data, String title, int height, int width, String savingName ) throws IOException {

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


    // ------------------------------- BAR CHART ---------------------------------- //


    public static void  getBarChart(Map<String,Long> data, String title,
                                    String xlabel,String ylabel,int count, int height, int width, String savingName) throws IOException {

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
















}
