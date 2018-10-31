package com.Project.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * StreamGetUserOfHashtags gets hashtags and the user who used them in a tweet.
 * The output looks like ([HASHTAG],[User])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class StreamGetUserOfHashtags {
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
		}

		//do streaming-operation
		DataStream<Tuple2<String, String>> tweets = streamSource
				.flatMap(new SelectTweetsWithHashtagsAndUsername())
				.keyBy(0).
				reduce(new GetAllUser());

		// emit result
		tweets.print();

		// execute program
		env.execute("Twitter Streaming Example");

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
}
