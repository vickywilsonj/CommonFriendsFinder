package com.vwj.learning.java.problems.mapreduce.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vicky Wilson Jacob
 * @date 2017-08-03
 *
 */
public class FriendsReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text output = new Text();
		ArrayList<String[]> friendsArr = new ArrayList<String[]>();
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String input = it.next().toString();
			String[] allFriends = input.split(" ");
			friendsArr.add(allFriends);
		}
		String mutualFriends = "\t";
		// find common friend
		Set<String> commonFriend = new HashSet<String>();
		for (String[] friends : friendsArr) {
			Arrays.sort(friends);
			for (String friend : friends) {
				if (commonFriend.contains(friend))
					mutualFriends += friend + " ";
				else
					commonFriend.add(friend);
			}
		}
		output.set(mutualFriends);
		context.write(key, output);
	}
}
