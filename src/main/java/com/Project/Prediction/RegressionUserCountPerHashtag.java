package com.Project.Prediction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * RegressionUserCountPerHashtag calculates the simple regression for the amount 
 * of users that uses a hashtag.
 * The output looks like ([HASHTAG],[USERCOUND],[PREDICTED-USERCOUND)
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 * (silverscreamsnadine,2,2.0)
 */
public class RegressionUserCountPerHashtag {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(params.getInt("parallelism", 1));

		DataStream<String> streamSource = null;
		SimpleRegression regression = new SimpleRegression();
		
		if (params.has(TwitterSource.CONSUMER_KEY) &&
				params.has(TwitterSource.CONSUMER_SECRET) &&
				params.has(TwitterSource.TOKEN) &&
				params.has(TwitterSource.TOKEN_SECRET)
				) {
			streamSource = env.addSource(new TwitterSource(params.getProperties()));
			System.out.println("juhu");
		} else {
			System.out.println("Executing TwitterStream example with default props.");
			System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
					"--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
			// get default test text data
			//streamSource = env.fromElements(TwitterExampleData.TEXTS);
		}
		
		WindowedStream<Tuple3<String, String, Integer>, Tuple, TimeWindow> tweets = streamSource
				.flatMap(new SelectTweetsWithHashtagsAndUsername())
				.keyBy(0)
				.timeWindow(Time.seconds(10));

		//compares the predicted value to all results of the timeWindow (20 second)
		tweets.apply(new WindowFunction<Tuple3<String,String,Integer>, Tuple3<String, Integer, Double>, Tuple, TimeWindow>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, Integer>> input,
					Collector<Tuple3<String,Integer, Double>> out) throws Exception {
				//a hashset to only have unique user
				HashSet<String> user = new HashSet<String>();
				String hashtag = "";
				// TODO Auto-generated method stub
				int count = 0;
				//iterates through all results of the window
				for (Tuple3<String, String, Integer> e : input) {
					//get the hashtag once
					if(hashtag.equals("")) {
						hashtag = e.f0;
					}
					user.add(e.f1);
					count += 1;
				}
				//calculate prediction
				regression.addData(count, user.size());
				double pred = regression.getIntercept() + regression.getSlope() * count;
				//if prediction available
				if(!Double.isNaN(pred)) {
					out.collect(new Tuple3<String,Integer, Double>(hashtag,user.size(), pred));
				}
			}
		}).print();

		env.execute("Twitter Streaming Example");

	}

	public static class Flatten implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws Exception {
			out.collect(new Tuple2<String, String>(value.f0, value.f1));
		}

	}

	public static class SelectTweetsWithHashtagsAndUsername implements FlatMapFunction<String, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
			if(hasHashtags) {
				String user = jsonNode.get("user").get("name").asText();
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String hashTagName = jsonNode2.get("text").asText();
						out.collect(new Tuple3<String, String, Integer>(hashTagName.toLowerCase(), user, 1));
					}
				}
			}
		}

	}
}

