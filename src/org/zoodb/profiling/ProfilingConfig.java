package org.zoodb.profiling;

public class ProfilingConfig {
	
	public static int LOB_TRESHOLD = 4096;
	
	public static final int COST_NEW_REFERENCE = 17;
	
	/*
	 * Analyzers
	 */
	public static boolean ENABLE_QUERY_PROFILING = true;
	
	public static boolean ENABLE_ANALYZER_CLASS_SPLIT = true;
	
	public static boolean ENABLE_ANALYZER_SHORTCUTS = true;
	
	public static boolean ENABLE_ANALYZER_AGGREGATION = true;
	
	public static boolean ENABLE_ANALYZER_UNUSED_COLLECTION = true;
	
	public static boolean ENABLE_ANALYZER_UNUSED_FIELDS = true;
	
	public static boolean ENABLE_ANALYZER_LOB = true;
}
