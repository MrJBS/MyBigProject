package com.Project.Charts;

import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import org.apache.flink.api.java.tuple.Tuple2;


public class JavaFxChart extends Application {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override public void start(Stage stage) throws IOException {

		BufferedReader br;
		
		Map<String, List<Tuple2<Double, Long>>> map = new HashMap<String, List<Tuple2<Double, Long>>>();

		br = new BufferedReader(new FileReader("C:/Users/jagsb/Projects/BigData/Flink/src/main/java/com/Project/continious-stream-data-time-19-10-1700"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String cleared = line.replaceAll("[\"()]","");
			String[] splitted = cleared.split(",");
			String key = splitted[0];
			Double avg = Double.parseDouble(splitted[1]);
			Long time = Long.parseLong(splitted[3]);
			if(!map.containsKey(splitted[0])) {
				map.put(splitted[0], new ArrayList<Tuple2<Double, Long>>());
			}
			
			List<Tuple2<Double, Long>> list = map.get(key);	
			Tuple2<Double, Long> tup =new Tuple2<Double, Long>(avg, Math.abs(time-System.currentTimeMillis())- 4289999);
			list.add(tup);
		}
		
		
		br.close();

		stage.setTitle("Line Chart Sample");
		//defining the axes
		final NumberAxis time = new NumberAxis();
		final NumberAxis average = new NumberAxis();
		time.setLabel("Average per time");
		//creating the chart
		final LineChart<Number,Number> lineChart = 
				new LineChart<Number,Number>(time,average);

		lineChart.setTitle("Length of Tweets per Country");
		//defining a series

		//populating the series with data
		
		for (Entry<String, List<Tuple2<Double, Long>>> entry : map.entrySet())
		{
		    System.out.println(entry.getKey() + "/" + entry.getValue());
			XYChart.Series series = new XYChart.Series();
			series.setName(entry.getKey());
			for (Tuple2<Double, Long> item : entry.getValue()) {
				series.getData().add(new XYChart.Data(item.f1, item.f0));
			}
			lineChart.getData().add(series);
		}

		Scene scene  = new Scene(lineChart,800,600);

		stage.setScene(scene);
		stage.show();
	}
	

	public static void main(String[] args) {
		launch(args);
	}
}