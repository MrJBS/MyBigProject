package com.Project.Batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
 * BatchGetHashesFromLocation gets hashtags posted in a countries.
  * The output looks like ([Country: Hashtags])
 * @author  Jonas Schneider
 * @version 1.0
 * @since   30.10.2018
 */
public class BatchGetHashesFromLocation {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(params.get("output"));

		DataSet<String> result = tweetText
				.flatMap(new SelectTweetsWithHashtags())
				.groupBy(0)
				.reduceGroup(new Reduce());

		result.print();
	}

	/**
	 * combines all hashtags into one string
	 */
	public static class Reduce implements GroupReduceFunction<Tuple2<String,String>, String>{
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple2<String, String>> values, Collector<String> out)
				throws Exception {
			// TODO Auto-generated method stub
			String country = "";
			String hashtags = "";

			for (Tuple2<String, String> v : values) {
				if(country.equals("")) {
					country = v.f0 + ":";
				}
				hashtags +=  v.f1 + ";";							
			}
			out.collect(country + hashtags);
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
