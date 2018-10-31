package com.Project.Batch;


import java.util.ArrayList;
import java.util.Arrays;
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
 * BatchGetUserOfHashtags gets hashtags and the user who used them in a tweet.
 * The output looks like ([HASHTAG],[User])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class BatchGetUserOfHashtags {


	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> dataset = env.readTextFile(params.get("output"));

		DataSet<Tuple2<String, String>> counts = dataset
				.flatMap(new SelectTweetsWithHashtagsAndUsername())
				.groupBy(0).
				reduce(new GetAllUser());
		counts.print();
	}

	/**
	 * Combines all unique users of a hashtag into one string and removes duplicates 
	 */
	public static class GetAllUser implements ReduceFunction<Tuple2<String,String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> value1,
				Tuple2<String, String> value2) throws Exception {

			List<String> list = new ArrayList<String>(Arrays.asList(value1.f1.split(";")));
			if(!list.contains(value2.f1)) {
				return new Tuple2<String, String>(value1.f0, (value1.f1 + ";" + value2.f1));
			}
			return value1;
		}
	}
	
	/**
	 * removes the integer of the input
	 */
	public static class Flatten implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws Exception {
			out.collect(new Tuple2<String, String>(value.f0, value.f1));
		}

	}

	/**
	 * Parsing of json-text(tweet) and returns a Tuple2 with parsed hashtag and tweet-text.
	 */
	public static class SelectTweetsWithHashtagsAndUsername implements FlatMapFunction<String, Tuple2<String, String>> {
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
				String user = jsonNode.get("user").get("name").asText();
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String hashTagName = jsonNode2.get("text").asText();
						out.collect(new Tuple2<String, String>(hashTagName.toLowerCase(), user));
					}
				}
			}
		}

	}

}


