package org.zoodb.jdo.stuff;

import java.util.logging.Logger;

public class DatabaseLogger {

	private static final Logger _LOGGER = 
		Logger.getLogger(DatabaseLogger.class.getName());

    private static int _verbosityLevel = -1;
    private static boolean _verboseToLog = false;
	
	/**
	 * Prints a debug message if the level is below or equal the 
	 * <code>verbosity</code> setting. The output can be
	 * redirected to the logging mechanism by setting the following
	 * property: <tt>verboseOutput = log</tt>.
	 * @param level
	 * @param message Message to print.
	 */
	public static final void debugPrint(int level, String ... message) {
		if (level <= _verbosityLevel) {
			FormattedStringBuilder buf = 
				new FormattedStringBuilder().append("Debug: ").append(message);
			if (_verboseToLog) {
				_LOGGER.info(buf.toString());
			} else {
				System.out.print(buf.toString());
			}
		}
	}

	/**
	 * Prints a debug message if the level is below or equal the 
	 * <code>verbosity</code> setting. The output can be
	 * redirected to the logging mechanism by setting the following
	 * property: <tt>verboseOutput = log</tt>.
	 * @param level
	 * @param message Message to print.
	 */
	public static final void debugPrintln(int level, String ... message) {
		debugPrint(level, message);
		if (level <= _verbosityLevel && !_verboseToLog) {
			System.out.println();
		}
	}

	public static void severe(String string) {
		System.out.println(string);
	}
}
