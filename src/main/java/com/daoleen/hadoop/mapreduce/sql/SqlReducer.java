package com.daoleen.hadoop.mapreduce.sql;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class SqlReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	private final Logger logger = Logger.getLogger(SqlReducer.class);
	private final JsonParser parser = new JsonParser();
	private final Text jsonResult = new Text();
	private JsonElement order = null;
	private JsonArray lineItems = new JsonArray();
	
	@Override
	protected void reduce(IntWritable id, Iterable<Text> jsons, Context context)
			throws IOException, InterruptedException {
				
		logger.info("In Reducer:");
		logger.info("Id is: " + id);
		logger.info("Jsons are: " + jsons);
		
		splitJsonsByRecordType(jsons);
		
		logger.info("order = " + ((order == null) ? "null" : order.toString()));
		logger.info("size of lineItems is: " + lineItems.size());
		
		if(order != null && lineItems.size() > 0) {
			for(JsonElement lineItem : lineItems) {
				String orderLineItem = getOrderLineItemString(lineItem);
				jsonResult.set(orderLineItem);
				logger.info("result row: " + jsonResult.toString());
				context.write(NullWritable.get(), jsonResult);
			}
		}
	}
	
	
	private void splitJsonsByRecordType(Iterable<Text> jsons) {
		for(Text json : jsons) {
			logger.info("json row: " + json.toString());
			JsonElement jsonRecond = parser.parse(json.toString());
			JsonArray jsonRecordColumns = jsonRecond.getAsJsonArray();
			
			if(jsonRecordColumns.get(0).toString().equals("\"order\"")) {
				order = jsonRecond;
			}
			else {
				lineItems.add(jsonRecordColumns);
			}
		}
	}
	
	
	private String getOrderLineItemString(JsonElement lineItem) {
		JsonArray orderLineItem = new JsonArray();
		orderLineItem.addAll(order.getAsJsonArray());
		orderLineItem.addAll(lineItem.getAsJsonArray());
		return orderLineItem.toString();
	}
}
