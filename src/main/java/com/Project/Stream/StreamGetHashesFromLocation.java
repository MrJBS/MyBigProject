package com.Project.Stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * StreamGetHashesFromLocation gets hashtags posted in a countries.
 * The output looks like ([Country: Hashtags])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class StreamGetHashesFromLocation {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
	
		final ParameterTool params = ParameterTool.fromArgs(args);

		//prepare evironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(params.getInt("parallelism", 1));

		DataStream<String> streamSource = null;

		//check twitter parameter
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
		@SuppressWarnings("deprecation")
		DataStream<String> result = streamSource
		.flatMap(new SelectTweetsWithHashtags())
		.keyBy(0)
		.fold("", new Fold())
		;

		result.print();

		// execute program
		env.execute("Twitter Streaming Example");
	}

	/**
	 * creates the String with the different hashes of the countrie
	 */
	@SuppressWarnings("deprecation")
	public static class Fold implements FoldFunction<Tuple2<String,String>, String>{

		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String accumulator, Tuple2<String, String> value) throws Exception {
			accumulator = accumulator.equals("")? value.f0 + ":": accumulator;
			accumulator += value.f1  + ";";
			return accumulator;
		}
	}

	/**
	 * Parsing of json-text(tweet) and returns a Tuple2 with parsed hashtag and a Country.
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
