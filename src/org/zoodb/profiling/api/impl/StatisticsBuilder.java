package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IPathManager;

import ch.ethz.globis.profiling.commons.statistics.ClassStatistics;
import ch.ethz.globis.profiling.commons.statistics.FieldStatistics;

public class StatisticsBuilder {
	
	private Collection<ClassStatistics> classStats;
	
	private IPathManager pathM;
	
	public StatisticsBuilder() {
		classStats = new LinkedList<ClassStatistics>();
		pathM = ProfilingManager.getInstance().getPathManager();
	}
	
	public Collection<ClassStatistics> buildClassStatistics() {
		Iterator<Class<?>> archiveIterator = pathM.getClassIterator();
		
		Class<?> currentClass = null;
		ClassStatistics currentCS = null;
		while (archiveIterator.hasNext()) {
			currentClass = archiveIterator.next();
			ActivationArchive aa = pathM.getArchive(currentClass);
			
			currentCS = new ClassStatistics();
			currentCS.setClassName(currentClass.getName());
			currentCS.setAvgSize(Math.round(aa.getAvgObjectSize()));
			currentCS.setTotalActivations(aa.size());
			currentCS.setTotalWrites(getTotalWritesForClass(currentClass));
			
			currentCS.setFieldStatistics(getFieldStatisticsForClass(currentClass));
			
			classStats.add(currentCS);
		}
		
		return classStats;
		
	}
	
	private int getTotalWritesForClass(Class<?> c) {
		return 0;
	}
	
	
	private Collection<FieldStatistics> getFieldStatisticsForClass(Class<?> currentClass) {
		Collection<FieldStatistics> fs = new LinkedList<FieldStatistics>();
		
		ClassSizeManager csm = ProfilingManager.getInstance().getClassSizeManager();
		ClassSizeStats css = csm.getClassStats(currentClass);
		
		FieldStatistics currentFS = null;
		for (String s : css.getAllFields()) {
			currentFS = new FieldStatistics();
			currentFS.setFieldName(s);
			
			int[] fieldData = getFieldData(currentClass,s);
			currentFS.setTotalReads(fieldData[0]);
			currentFS.setTotalWrites(fieldData[1]);
			
			
			fs.add(currentFS);
		}		
		
		return fs;
	}
	
	private int[] getFieldData(Class<?> c, String field) {
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager();
		return fm.getRWCount(c, field);
	}

}
