package com.Project.Stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
* The StreamLongestWordPerHashtag finds the longes word in a tweet per hashtag by stream.
* The application is using a timewindow. 
* The output looks like ([HASHTAG],[WORD], [LENGTH])
* @author  Jonas Schneider
* @version 1.0
* @since   30.10.2018
*/
public class StreamLongestWordPerHashtag {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		
//		Arguments for twitter connection and location of output file
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

		DataStream<Tuple3<String, String, Integer>> tweets = streamSource
	
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new FlatToWords() )
				.keyBy(0)
				.timeWindow(Time.seconds(20))
				.maxBy(2);

		tweets.print();

		// execute program
		env.execute("Twitter Streaming Example");
	}

	/**
	 * Filter that finds all tweets with one of the given hashtags.
	 */
	public static class FilterTweets implements FilterFunction<Tuple2<String, String>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private List<String> list;
		
		public FilterTweets() {
			list = new ArrayList<>();
			list.add("exo");
			list.add("baekhyun");
			list.add("wasteitonme");
			list.add("tempo_baekhyun");
			list.add("bts");
			}
		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			
			String s = value.f0.toLowerCase();

			for(String hash : list) {
				if(s.equals(hash)) {
					return true;
				}
			}
			
			return false;
		}
		
	}
	
	/**
	 * Parsing of json-text(tweet) and returns a Tuple2 with parsed hashtag and tweet-text.
	 */
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
				JsonNode b = jsonNode.get("entities").get("hashtags");
				for (JsonNode jsonNode2 : b) {
					if(jsonNode2.has("text")) {
						String hashTagName = jsonNode2.get("text").asText();
						String hashTagText = jsonNode.get("text").asText();
						out.collect(new Tuple2<String, String>(hashTagName.toLowerCase(), hashTagText));
					}
				}
			}
		}

	}
	
	/**
	 * returns all Words of a tweet-text 
	 */
	public static class FlatToWords implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String[] words = value.f1.split("(\\s|\\b)");
			
			for (String w : words) {
				out.collect(new Tuple3<String, String, Integer>(value.f0, w, w.length()));
			}
		}

	}
}
