package com.Project.Comparison;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * The output looks like ([COUNTRY],Batch-Value: [NONE|HASHTAG]  Stream-Value:[HASHTAG])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class CompareHashesFromLocation {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		//batch-processing
		List<Tuple2<String, String>> countries = batchProcessing(params.get("output"));
		//printing number of results in batch
		System.out.println(countries.size());

		//because batch-values are unique we push results into
		//the hashmap for easier comparison
		Map<String, String> hashmap = new HashMap<String, String>();
		for (Tuple2<String, String> c : countries) {
			hashmap.put(c.f0, c.f1);
		}


		//prepare evironment for streaming
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.getConfig().setGlobalJobParameters(params);
		streamEnv.setParallelism(params.getInt("parallelism", 1));

		//streaming-results
		DataStream<Tuple2<String, String>> streamresult = streamPrcessing(params, streamEnv, hashmap);

		//printing results
		streamresult.print();

		// execute program
		streamEnv.execute("Twitter Streaming Example");

	}

	private static DataStream<Tuple2<String, String>> streamPrcessing(ParameterTool params, StreamExecutionEnvironment streamEnv, Map<String, String> hashmap) {
		DataStream<String> streamSource = null;
		streamSource = streamEnv.addSource(new TwitterSource(params.getProperties()));

	
		DataStream<Tuple2<String, String>> streamresult= streamSource
		.flatMap(new SelectTweetsWithHashtags())
		.keyBy(0)
		.reduce(new ReduceFunction<Tuple2<String,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2)
					throws Exception {
				// TODO Auto-generated method stub
				String country = value1.f0;
				String hashtags = value1.f1 + ";" + value2.f1;
				return new Tuple2<String,String>(country,hashtags);
			}
		})
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

		return streamresult;
	}

	private static List<Tuple2<String, String>> batchProcessing(String path) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(path);

		DataSet<Tuple2<String, String>> result = tweetText
		.flatMap(new SelectTweetsWithHashtags())
		.groupBy(0)
		.reduceGroup(new GroupReduceFunction<Tuple2<String,String>, Tuple2<String,String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String,String>> out)
					throws Exception {
				// TODO Auto-generated method stub
				String country = "";
				String hashtags = "";

				for (Tuple2<String, String> v : values) {
					if(country.equals("")) {
						country = v.f0;
					}
					hashtags += v.f1 + ";";							
				}
				out.collect(new Tuple2<String,String>(country,hashtags));

			}

		});

		List<Tuple2<String, String>> countries = result.collect();

		return countries;
	}

	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags") && jsonNode.has("place");
			if(hasHashtags) {
				JsonNode location = jsonNode.get("place");
				if(!location.isNull()) {
					JsonNode b = jsonNode.get("entities").get("hashtags");
					String country = location.get("country").asText();
					for (JsonNode jsonNode2 : b) {
						String hashTagName = jsonNode2.get("text").asText();
						out.collect(new Tuple2<String, String>(country, hashTagName.toLowerCase()));
					}
				}
			}
		}

	}

}

