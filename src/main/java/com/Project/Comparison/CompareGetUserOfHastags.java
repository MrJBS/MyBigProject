package com.Project.Comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * CompareGetUserOfHastags compares the results of streaming and batch-operation.
 * The output looks like ([HASHTAG],Batch-Value: [NONE|USER-FROM-BATCH]  Stream-Value:[USER-FROM-STREAM])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class CompareGetUserOfHastags {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		//get results from batch operation
		List<Tuple2<String, String>> counts = batchProcessing(params.get("output"));
		//prints amount of results
		System.out.println("Number of Elements computed from batchfile" + counts.size());

		//push all results to a map for easy comparison
		Map<String, String> hashmap = new HashMap<String, String>();
		for (Tuple2<String, String> tuple2 : counts) {
			hashmap.put(tuple2.f0, tuple2.f1);
		}

		//parameter for streaming
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.getConfig().setGlobalJobParameters(params);
		streamEnv.setParallelism(params.getInt("parallelism", 1));

		// get stream-data and compare with batch data stored in hashmap
		DataStream<Tuple2<String, String>> tweets = streamProcessing(params, hashmap, streamEnv);

		tweets.print();

		// execute program
		streamEnv.execute("Twitter Streaming Example");

	}

	public static class Flatten implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws Exception {
			out.collect(new Tuple2<String, String>(value.f0, value.f1));
		}

	}

	public static class SelectTweetsWithHashtagsAndUsername implements FlatMapFunction<String, Tuple2<String, String>> {
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

	public static DataStream<Tuple2<String, String>> streamProcessing(ParameterTool params, Map<String, String> hashmap, StreamExecutionEnvironment streamEnv ) {

		DataStream<String> streamSource = null;
		streamSource = streamEnv.addSource(new TwitterSource(params.getProperties()));

		DataStream<Tuple2<String, String>> tweets = streamSource
				.flatMap(new SelectTweetsWithHashtagsAndUsername())
				.keyBy(0)
				//find all unique user per hashtag
				.reduce(new ReduceFunction<Tuple2<String,String>>() {
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
				})
				.keyBy(0)
				.timeWindowAll(Time.seconds(10))
				//run thought all results of the 10 seconds window
				//and search for entry in batch-data and create String to compare results
				.process(new ProcessAllWindowFunction<Tuple2<String,String>, Tuple2<String, String>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							ProcessAllWindowFunction<Tuple2<String, String>, Tuple2<String, String>, TimeWindow>.Context context,
							Iterable<Tuple2<String, String>> elements, Collector<Tuple2<String, String>> out)
									throws Exception {
						for (Tuple2<String, String> e : elements) {
							String batchvalue = "NONE";
							if(hashmap.containsKey(e.f0)) {
								batchvalue = hashmap.get(e.f0);
							}
							String text = "Batch-Value: " + batchvalue + "  Stream-Value:" + e.f1;
							out.collect(new Tuple2<String, String>(e.f0, text));
						}
					}
				});

		return tweets;
	}

	public static List<Tuple2<String, String>> batchProcessing(String path) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> dataset = env.readTextFile(path);

		DataSet<Tuple2<String, String>> results = dataset
				.flatMap(new SelectTweetsWithHashtagsAndUsername())
				.groupBy(0).
				reduce(new ReduceFunction<Tuple2<String,String>>() {

					/**
					 * 
					 */
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
				});
		List<Tuple2<String, String>> list = results.collect();
		return list;
	}

}
