/**
 * 
 */
package com.vwj.learning.java.problems.mapreduce.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vicky Wilson Jacob
 * @date 2017-08-03
 *
 */
public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "\n");
		String input = null;
		String[] rowSplit = null;
		String[] friends = null;
		while (stringTokenizer.hasMoreTokens()) {
			input = stringTokenizer.nextToken();
			rowSplit = input.split("->");
			friends = rowSplit[1].split(" ");
			for (int i = 0; i < friends.length; i++) {
				String[] temp = {rowSplit[0],friends[i]};
				Arrays.sort(temp);
				context.write(new Text(temp[0] + " " + temp[1]), new Text(rowSplit[1]));
			}
		}
	}
}
