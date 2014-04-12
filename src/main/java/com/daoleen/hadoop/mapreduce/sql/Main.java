package com.daoleen.hadoop.mapreduce.sql;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonElement;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		JobConf conf = new JobConf();
		String[] remainingArgs = new GenericOptionsParser(args).getRemainingArgs();
		
		File outputDir = new File(remainingArgs[1]);
		if(outputDir.exists()) {
			FileUtils.deleteDirectory(outputDir);
		}
		
		if(remainingArgs.length != 2) {
			System.out.println("Usage: <in_dir> <out_dir>");
			System.exit(0);
		}
		
		Job job = new Job(conf, "sql job");
		job.setMapperClass(SqlMapper.class);
		job.setReducerClass(SqlReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
		
		job.waitForCompletion(true);
	}

}
