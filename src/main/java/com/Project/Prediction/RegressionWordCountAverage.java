package com.Project.Prediction;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.operators.translation.Tuple3UnwrappingIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * RegressionWordCountAverage calculates the simple regression for the average of
 * counted words in a hashtag
 * The output looks like ([HASHTAG],[Average],[PREDICTED-AVRG)
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 * (silverscreamsnadine,2,2.0)
 */
public class RegressionWordCountAverage {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(params.getInt("parallelism", 1));

		DataStream<String> streamSource = null;

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

		SimpleRegression regression = new SimpleRegression();
		
		DataStream<Tuple3<String, Double, Double>> tweets = streamSource

				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new WordCount())
				.keyBy(0)
				.timeWindow(Time.seconds(10))
				.reduce(new MyReduceFunction())
				.process(new ProcessFunction<Tuple4<String,Double,Integer,Long>, Tuple3<String, Double, Double>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2018098507433484199L;
						
					
					@Override
					public void processElement(Tuple4<String, Double, Integer, Long> value,
							ProcessFunction<Tuple4<String, Double, Integer, Long>, Tuple3<String, Double, Double>>.Context ctx,
							Collector<Tuple3<String, Double, Double>> out) throws Exception {
						// TODO Auto-generated method stub
						regression.addData(value.f2, value.f1);
						double expPoint = regression.getIntercept() + regression.getSlope() * value.f2;

						if(!Double.isNaN(expPoint)) {
							out.collect(new Tuple3<String, Double, Double>(value.f0, value.f1, expPoint));
						}

						//new Tuple3<String, Double, Double>(value.f0, value.f1, (regression.toString() + regression.getSlope() * value.f2))
					}
				}).setParallelism(1);


		// emit result
		if (params.has("output") && false) {
			tweets.writeAsText(params.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}


	public static class MyReduceFunction implements ReduceFunction<Tuple4<String,Double,Integer, Long>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple4<String, Double, Integer, Long> reduce(Tuple4<String, Double, Integer, Long> value1,
				Tuple4<String, Double, Integer, Long> value2) throws Exception {
			String key = value1.f0;
			double everage = value1.f1;
			int count = value1.f2;
			int newCount = count + 1;
			double newEverage = ((count * everage) + value2.f1) / newCount;
			return new Tuple4<String, Double, Integer, Long>(key, newEverage, newCount, value2.f3);
		}

	}

	public static class Transform implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value.f0, 1));	
		}

	}

	public static class FilterTweets implements FilterFunction<Tuple2<String, String>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private List<String> list;

		public FilterTweets() {
			list = new ArrayList<>();
			list.add("\"germany\"");
			list.add("\"russia\"");
			list.add("\"usa\"");
			list.add("\"japan\"");
			list.add("\"china\"");
		}
		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			String s = value.f0.toLowerCase();

			for(String hash : list) {
				if(s.contains(hash)) {
					return true;
				}
			}

			return true;
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
						String hashTagName = jsonNode2.get("text").toString();
						String hashTagText = jsonNode.get("text").toString();
						out.collect(new Tuple2<String, String>(hashTagName.toLowerCase(), hashTagText));
					}
				}
			}
		}

	}

	public static class WordCount implements FlatMapFunction<Tuple2<String, String>, Tuple4<String, Double, Integer, Long>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple4<String, Double, Integer, Long>> out) throws Exception {
			double count = value.f1.split(" ").length;
			out.collect(new Tuple4<String, Double, Integer, Long>(value.f0, count, 1, System.currentTimeMillis()));
		}

	}
}

