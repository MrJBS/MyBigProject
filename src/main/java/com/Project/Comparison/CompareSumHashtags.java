package com.Project.Comparison;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * CompareHashesFromLocation compares the results of streaming and batch-operation.
 * The output looks like ([HASHTAG],Batch-Value: [NONE|SUM]  Stream-Value:[SUM])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class CompareSumHashtags {

	public static void main(String[] args) throws Exception {
		//start parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		//process the batch
		List<Tuple2<String, Integer>> counts = batchProcess(params.get("output"));

		Map<String, Integer> hashmap = new HashMap<String, Integer>();
		for (Tuple2<String, Integer> tuple2 : counts) {
			hashmap.put(tuple2.f0, tuple2.f1);
		}

		System.out.println(hashmap.size());

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.getConfig().setGlobalJobParameters(params);
		streamEnv.setParallelism(params.getInt("parallelism", 1));

		
		DataStream<Tuple2<String, String>> tweets = streamProcess(params, hashmap, streamEnv);

		tweets.print();

		streamEnv.execute("Twitter Streaming Example");
	}


	private static DataStream<Tuple2<String, String>> streamProcess(ParameterTool params, Map<String, Integer> hashmap, StreamExecutionEnvironment streamEnv ) {
		DataStream<String> streamSource = streamEnv.addSource(new TwitterSource(params.getProperties()));

		DataStream<Tuple2<String, String>> tweets = streamSource
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new FlatTweets())
				.keyBy(0).sum(1)
				.timeWindowAll(Time.seconds(10))
				.process(new ProcessAllWindowFunction<Tuple2<String,Integer>, Tuple2<String, String>, TimeWindow>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, String>, TimeWindow>.Context context,
							Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, String>> out)
									throws Exception {
						for (Tuple2<String, Integer> e : elements) {
							String batchvalue = "NONE";
							if(hashmap.containsKey(e.f0)) {
								batchvalue = hashmap.get(e.f0).toString();
							}
							String text = "Batch-Value: " + batchvalue + "  Stream-Value:" + e.f1;
							out.collect(new Tuple2<String, String>(e.f0, text));
						}

					}

				});
		return tweets;
	}


	private static List<Tuple2<String, Integer>> batchProcess(String path) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(path);

		List<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				tweetText
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new FlatTweets())
				.groupBy(0)
				.sum(1).collect();
		return counts;
	}


	public static class FilterTweets implements FilterFunction<Tuple3<String, String, String>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<String, String, String> value) throws Exception {
			if(value.f1.length() > 0 && value.f2.equals("en")) {
				return true;
			}
			return false;
		}

	}

	public static class FlatTweets implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple3<String, String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<String, Integer>(value.f0, 1));
		}

	}

	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple3<String, String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
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
