package com.daoleen.hadoop.mapreduce.sql;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class SqlMapper extends Mapper<Object, Text, IntWritable, Text> {
	private final Logger logger = Logger.getLogger(SqlMapper.class);
	private final IntWritable id = new IntWritable();
	private final JsonParser parser = new JsonParser();
	
	@Override
	protected void map(Object source, Text value, Context context)
			throws IOException, InterruptedException {

		logger.info("Key for mapper is: " + source);
		logger.info("Value for mapper is:" + value);

		JsonElement jsonRow = parser.parse(value.toString());
		JsonArray jsonRecordColumns = jsonRow.getAsJsonArray();
		logger.info("JsonRecordColumns is: " + jsonRecordColumns);
	
		JsonElement recordId = jsonRecordColumns.get(1);
		id.set(recordId.getAsInt());
		logger.info("Record id is: " + recordId);
		
		context.write(id, value);
	}
}