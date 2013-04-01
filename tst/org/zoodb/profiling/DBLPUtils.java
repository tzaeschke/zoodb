package org.zoodb.profiling;

import java.util.Date;
import java.util.Random;

public class DBLPUtils {
	
	
	private static Random rand = new Random();
	
	
	
	/**
	 * Returns a random number between between 1 and 5
	 * @return
	 */
	public static int getRating() {
		return rand.nextInt(5) + 1;
	}
	
	public static Date getDate() {
		return new Date(System.currentTimeMillis());
	}

	
	public static String getRandomString() {
		return randomAlphabetic(1024);
	}
	
	public static String getRandomString(int length) {
		return randomAlphabetic(length);
	}
	
	private static String randomAlphabetic(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			//valid chars: 32-126
			char c = (char) (rand.nextInt(94) + 32);
			sb.append(c);
		}
		return sb.toString();
	}
}
