package org.zoodb.profiling;

import org.zoodb.jdo.api.ZooHelper;

public class ProfilingConfig {
	
	public static final double ANALYZERS_GAIN_COST_RATIO_THRESHOLD = 0.5;
	
	public static final int COST_NEW_REFERENCE = 17;
	public static boolean ENABLE_QUERY_PROFILING = true;
	
	
	/*
	 * Analyzers
	 */
	public static boolean ENABLE_ANALZYER_CLASS_MERGE = true;
	public static boolean ENABLE_ANALYZER_CLASS_SPLIT = true;
	public static boolean ENABLE_ANALYZER_SHORTCUTS = true;
	public static boolean ENABLE_ANALYZER_AGGREGATION = true;
	public static boolean ENABLE_ANALYZER_UNUSED_COLLECTION = true;
	public static boolean ENABLE_ANALYZER_UNUSED_FIELDS = true;
	public static boolean ENABLE_ANALYZER_UNUSED_CLASSES = true;
	public static boolean ENABLE_ANALYZER_LOB = true;
	
	/*
	 * ClassMergeAnalyzer settings
	 */
	public static Double CMA_ALPHA	= 1.0;
	public static Double CMA_BETA	= 1.0;
	public static Double CMA__GAMMA = 1.0;
	public static Double CMA_DELTA = 1.0;
	
	public static CMA_EPSILON_STRATEGY CMA_STRATEGY = CMA_EPSILON_STRATEGY.LOWER_BOUND;
	
	public static enum CMA_EPSILON_STRATEGY {
		LOWER_BOUND,
		UPPER_BOUND,
		MEAN
	};
	
	
	/*
	 * LOBAnalyzer
	 */
	public static int LOB_TRESHOLD = 1000;
	public static double LOB_THRESHOLD_DA_RATIO = 0.9;
	
	/*
	 * Split Analyzer
	 */
	public static final int SA_MIN_ATTRIBUTE_COUNT = 2;
	public static final long SA_MIN_OBJECT_SIZE = 2048;
	public static final int SA_MIN_WRITE_THRESHOLD = 10;
	
	/**
	 * Returns the root path where the configuration and profiler-exports are stored
	 * @return
	 */
	public static String getExportRootDir() {
		return ZooHelper.getDataStoreManager().getDefaultDbFolder();
	}
}
