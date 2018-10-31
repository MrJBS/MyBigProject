package com.Project.Batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/**
* The BatchSumHashtags counts the occurrence of each hashtags.
* The output looks like ([HASHTAG],[COUNT])
* @author  Jonas Schneider
* @version 1.0
* @since   30.10.2018
*/

public class BatchSumHashtags {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> tweetText = env.readTextFile(params.get("output"));

		DataSet<Tuple2<String, Integer>> counts =
				tweetText
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new FlatTweets())
				.groupBy(0)
				.sum(1);
		counts.print();
	}
	
	/**
	 * filters tweets with no tweet-text and not english-language 
	 */
	public static class FilterTweets implements FilterFunction<Tuple3<String, String, String>>{
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<String, String, String> value) throws Exception {
			if(value.f1.length() > 0 && value.f2.equals("en")) {
				return true;
			}
			return false;
		}
		
	}
	
	/**
	 * Prepares the element for the sum 
	 */
	public static class FlatTweets implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple3<String, String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<String, Integer>(value.f0, 1));
		}
		
	}
	
	/**
	 * Parsing of json-text(tweet) and returns a Tuple2 with parsed hashtag and tweet-text.
	 */
	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple3<String, String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {

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
						String language = jsonNode.get("lang").asText();
						out.collect(new Tuple3<String, String, String>(hashTagName.toLowerCase(), hashTagText, language));
					}
				}
			}
		}
	}
}