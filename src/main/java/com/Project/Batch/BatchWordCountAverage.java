package com.Project.Batch;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/**
* The BatchWordCountAverage program finds tweets to a list of hashtags
* and calculates its average words per tweet.
* Tweets are filtered.
* The output looks like ([HASHTAG],[AVERAGR],[AMOUNT_OF_TWEETS])
*
* @author  Jonas Schneider
* @version 1.0
* @since   30.10.2018
*/

public class BatchWordCountAverage {
	
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> dataset = env.readTextFile(params.get("output"));

		DataSet<Tuple3<String, Double, Integer>> counts = dataset
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new WordCount())
				// groups results by its hashtags 
				.groupBy(0)
				.reduce(new Reduce());
		// printing results to console
		counts.print();
	}

	/**
	 * calculates the average of words in a tweet
	 */
	public static class Reduce implements ReduceFunction<Tuple3<String,Double,Integer>>{

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> value1,
				Tuple3<String, Double, Integer> value2) throws Exception {
			// TODO Auto-generated method stub
			String key = value1.f0;
			double everage = value1.f1;
			int count = value1.f2;
			int newCount = count + 1;
			double newEverage = ((count * everage) + value2.f1) / newCount;
			return new Tuple3<>(key, newEverage, newCount);
		}
	}

	public static class Transform implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>{

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value.f0, 1));	
		}

	}
	/**
	 * Filter to find all tweets with one of the given hashtags.
	 */
	public static class FilterTweets implements FilterFunction<Tuple2<String, String>> {

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

			return false;
		}
	}

	/**
	 * Parsing of json-text(tweet) and returns a Tuple2 with parsed hashtag and tweet-text.
	 */
	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

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

	/**
	 * Counts words in the tweets text.
	 */
	public static class WordCount implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Double, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
			double count = value.f1.split(" ").length;
			out.collect(new Tuple3<String, Double, Integer>(value.f0, count, 1));
		}

	}
}



