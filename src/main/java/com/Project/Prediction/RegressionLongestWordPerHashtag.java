package com.Project.Prediction;


import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.Project.Stream.StreamLongestWordPerHashtag.FilterTweets;
import com.Project.Stream.StreamLongestWordPerHashtag.FlatToWords;
import com.Project.Stream.StreamLongestWordPerHashtag.SelectTweetsWithHashtags;

/**
 * RegressionLongestWordPerHashtag calculates the simple regression for the longest word in a hashtag.
 * The output looks like ([HASHTAG],[WORDSCOUNT],[LONGESTWORD],[PREDICTED-VALUE)
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class RegressionLongestWordPerHashtag {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		//results of the batch-computation
		List<Tuple3<String, Integer, Integer>> listOfResults = batchProcess(params.get("output"));

		//regression object
		SimpleRegression regression = new SimpleRegression();
		
		//create double array to feed regression-function
		double[][] data = new double[listOfResults.size()][2];
		for(int i = 0; i<listOfResults.size(); i++) {
			data[i][0] = (double)listOfResults.get(i).f1;
			data[i][1] = (double)listOfResults.get(i).f2;
		}
		//add data for comparison
		regression.addData(data);
		//regression data from the batch-computation
		Double intercept = regression.getIntercept();
		Double slope = regression.getSlope();

		//system environmant
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.getConfig().setGlobalJobParameters(params);
		streamEnv.setParallelism(params.getInt("parallelism", 1));

		//gets first part of streaming computation
		SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Double>> tweets = null;
		
		//process results of streaming to compare results with expected (regression) value
		tweets = streamProcess(params, streamEnv)
				.process(new ProcessFunction<Tuple3<String,Integer,Integer>, Tuple4<String,Integer,Integer,Double>>() {
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("unused")
					@Override
					public void processElement(Tuple3<String, Integer, Integer> value,
							ProcessFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, Double>>.Context ctx,
							Collector<Tuple4<String, Integer, Integer, Double>> out) throws Exception {
						System.out.println("");

						//calculate expected value for this tweet
						double predictionpoint = (intercept + slope * value.f1);
						//output 
						out.collect(new Tuple4<>(value.f0, value.f1,value.f2, predictionpoint));
					}
				});

		// Console output
		tweets.print();

		// execute program
		streamEnv.execute("Twitter Streaming Example");
	}

	private static DataStream<Tuple3<String, Integer, Integer>> streamProcess(ParameterTool params, StreamExecutionEnvironment streamEnv) {
		DataStream<String> streamSource = null;

		streamSource = streamEnv.addSource(new TwitterSource(params.getProperties()));

		DataStream<Tuple3<String, Integer, Integer>> tweets = streamSource

				.flatMap(new SelectTweetsWithHashtags())
				.flatMap(new FlatToWords())
				.flatMap(new FlatMapFunction<Tuple3<String,String,Integer>, Tuple3<String, Integer, Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Tuple3<String, String, Integer> value,
							Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
						out.collect(new Tuple3<>(value.f0, 1, value.f2));

					}
				})
				//remove 0 words
				.filter(new FilterFunction<Tuple3<String,Integer,Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
						if(value.f2>0)
							return true;
						return false;
					}
				})
				.keyBy(0)
				.timeWindow(Time.seconds(10))
				//find max and increment total hashtagcounts
				.reduce(new ReduceFunction<Tuple3<String,Integer,Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
							Tuple3<String, Integer, Integer> value2) throws Exception {
						int max = value1.f2;
						if(max < value2.f2)
							max = value2.f2;
						return new Tuple3<String, Integer, Integer>(value1.f0, value1.f1 + value2.f1, max);
					}
				});
		return tweets;
	}

	private static List<Tuple3<String, Integer, Integer>> batchProcess(String path) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(path);

		ReduceOperator<Tuple3<String, Integer, Integer>> counts = tweetText
				.flatMap(new SelectTweetsWithHashtags())
				.flatMap(new FlatToWords() )
				.groupBy(0)
				.getInputDataSet()
				.map(new MapFunction<Tuple3<String,String,Integer>, Tuple3<String,Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, Integer, Integer> map(Tuple3<String, String, Integer> value)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple3<>(value.f0, 1, value.f2);
					}

				})			
				//remove words with length = 0
				.filter(new FilterFunction<Tuple3<String,Integer,Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
						if(value.f2 >= 1)
							return true;
						return false;
					}
				})
				.groupBy(0)
				//find max value & increase count of words
				.reduce(new ReduceFunction<Tuple3<String,Integer,Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
							Tuple3<String, Integer, Integer> value2) throws Exception {
						int max = value1.f2;
						if(max < value2.f2)
							max = value2.f2;
						return new Tuple3<String, Integer, Integer>(value1.f0, value1.f1 + value2.f1, max);
					}
				});
		return counts.collect();
	}

	private double intercept;
	private double slope;

	public static class Transform implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>{

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value.f0, 1));	
		}

	}

	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
			if(hasHashtags) {
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String hashTagName = jsonNode2.get("text").asText();
						String hashTagText = jsonNode.get("text").asText();
						out.collect(new Tuple2<String, String>(hashTagName.toLowerCase(), hashTagText));
					}
				}
			}
		}
	}

	public static class FlatToWords implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String[] words = value.f1.split("(\\s|\\b)");
			for (String w : words) {
				out.collect(new Tuple3<String, String, Integer>(value.f0, w, w.length()));
			}
		}

	}
}

