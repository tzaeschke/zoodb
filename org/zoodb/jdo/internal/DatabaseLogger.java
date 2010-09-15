package org.zoodb.jdo.internal;

public class DatabaseLogger {

	private static int _verbosity = 3;
	
	public static void debugPrintln(int i, String string) {
		if (i < _verbosity) {
			System.out.println("Debug: " + string);
		}
		
	}

}
