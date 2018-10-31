package com.Project;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.google.gson.JsonArray;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.StringTokenizer;

public class TwitterTest {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
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

		DataStream<String> tweets = streamSource
				// selecting English tweets and splitting to (word, 1)
				//.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
				//.keyBy(0).sum(1)
				.filter(new FilterFunction<String>() {
					
					private transient ObjectMapper jsonParser;
					
					@Override
					public boolean filter(String value) throws Exception {
						
						if (jsonParser == null) {
							jsonParser = new ObjectMapper();
						}
						JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
						boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
						boolean hasText = jsonNode.has("text");
						boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
						if(hasHashtags) {
							String a = jsonNode.get("entities").get("hashtags").toString();
							JsonNode b = jsonNode.get("entities").get("hashtags");
							for (JsonNode jsonNode2 : b) {
								if(jsonNode2.has("text")) {
									String newww = jsonNode2.get("text").toString();
									
									if(newww.toLowerCase().contains("nfl") || newww.toLowerCase().contains("sports") || newww.toLowerCase().contains("nbl") || newww.toLowerCase().contains("sports") || newww.toLowerCase().contains("nba")) {
										return true;
									}
								}
							}
						}
						return true;
						}
				});

		// emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
			boolean hasText = jsonNode.has("text");
			boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
			if(hasHashtags) {
				String a = jsonNode.get("entities").get("hashtags").toString();
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String newww = jsonNode2.get("text").toString();
						out.collect(new Tuple2<>(value, 1));
					}
				}
				if(a.length()>2) {
					a.trim();
				}
			}
//			if (isEnglish && hasText) {
//				// message of tweet
//				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());
//
//				// split the message
//				while (tokenizer.hasMoreTokens()) {
//					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
//
//					if (!result.equals("")) {
//						out.collect(new Tuple2<>(result, 1));
//					}
//				}
//			}
		}

	}
}
