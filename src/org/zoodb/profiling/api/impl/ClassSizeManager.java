package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Map;

public class ClassSizeManager {
	
	private Map<Class<?>,ClassSizeStats> clazzStats;
	
	public ClassSizeManager() {
		clazzStats = new HashMap<Class<?>,ClassSizeStats>();
	}
	
	public ClassSizeStats getClassStats(Class<?> c) {
		ClassSizeStats css = clazzStats.get(c);
		
		if (css == null) {
			css = new ClassSizeStats();
			clazzStats.put(c, css);
		}
		return css;
	}

}
