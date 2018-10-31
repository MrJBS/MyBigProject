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

// Logs all received tweets from twitter
public class TwitterStreamLogger {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
//		Arguments for twitter connection and location of output file
//      --twitter-source.consumerKey C5ED8IZeE8uf1pBYBAD6fVlBr
//		--twitter-source.consumerSecret vxCM8NfN2xJtmrYQe08TjhoMaKTBuN5HEIopZakjTx3dsD0ONf
//		--twitter-source.token 1049954129886531584-I9n2BnNmajY5W3CQ0TkA6lWVfm6P4j
//		--twitter-source.tokenSecret bOZnd54AnVUpaHxozQg5bXvEVBlFdaGzn3UYpVhZrm6Rb
//		--output /home/jbs/Project/flink-training-exercises/src/main/java/com/Project/tweets
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

		// show or write results
		if (params.has("output")&&false) {
			streamSource.writeAsText(params.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			streamSource.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}
}
