package com.Project;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TwitterStreamImplementation {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		
//		Arguments for twitter connection and location of output file
//      --twitter-source.consumerKey C5ED8IZeE8uf1pBYBAD6fVlBr
//		--twitter-source.consumerSecret vxCM8NfN2xJtmrYQe08TjhoMaKTBuN5HEIopZakjTx3dsD0ONf
//		--twitter-source.token 1049954129886531584-I9n2BnNmajY5W3CQ0TkA6lWVfm6P4j
//		--twitter-source.tokenSecret bOZnd54AnVUpaHxozQg5bXvEVBlFdaGzn3UYpVhZrm6Rb
//		--output /home/jbs/Project/flink-training-exercises/src/main/java/com/Project/tweets
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

		DataStream<Tuple3<String, Double, Integer>> tweets = streamSource
	
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new WordCount()).keyBy(0)
				.reduce(new ReduceFunction<Tuple3<String,Double,Integer>>() {
					
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
				});


		// emit result
		if (params.has("output") && false) {
			tweets.writeAsText(params.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	public static class Transform implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value.f0, 1));	
		}
		
	}
	
	public static class FilterTweets implements FilterFunction<Tuple2<String, String>> {
		/**
		 * 
		 */
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
			
			String test = "test";
			String s = value.f0.toLowerCase();

			for(String hash : list) {
				if(s.contains(hash)) {
					return true;
				}
			}
			
			return false;
		}
		
	}
	
	public static class SelectTweetsWithHashtags implements FlatMapFunction<String, Tuple2<String, String>> {
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
				String a = jsonNode.get("entities").get("hashtags").toString();
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String hashTagName = jsonNode2.get("text").toString();
						String hashTagText = jsonNode.get("text").toString();
						out.collect(new Tuple2<String, String>(hashTagName, hashTagText));
					}
				}
			}
		}

	}
	
	public static class WordCount implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Double, Integer>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
			double count = value.f1.split(" ").length;
			out.collect(new Tuple3<String, Double, Integer>(value.f0, count, 1));
		}

	}
}

