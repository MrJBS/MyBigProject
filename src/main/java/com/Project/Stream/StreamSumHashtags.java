package com.Project.Stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * The StreamSumHashtags counts the occurrence of each hashtags.
 * The output looks like ([HASHTAG],[COUNT])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class StreamSumHashtags {

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

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new FlatTweets())
				.keyBy(0)
				.sum(1);
		
		// emit result
		tweets.print();

		// execute program
		env.execute("Twitter Streaming Example");
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

		/**
		 * 
		 */
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
