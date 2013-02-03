package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IPathManager;

import ch.ethz.globis.profiling.commons.statistics.ClassStatistics;
import ch.ethz.globis.profiling.commons.statistics.FieldStatistics;
import ch.ethz.globis.profiling.commons.statistics.PathStatItem;

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
			Collection<PathStatItemActivation> accessors = new LinkedList<PathStatItemActivation>();
			
			currentClass = archiveIterator.next();
			ActivationArchive aa = pathM.getArchive(currentClass);
			
			currentCS = new ClassStatistics();
			currentCS.setClassName(currentClass.getName());

			currentCS.setTotalActivations(aa.size());
			
			Iterator<AbstractActivation> ai = aa.getIterator();
			
			while (ai.hasNext()) {
				AbstractActivation current = ai.next();
				addToAccessors(current,accessors);
			}
			//attache accessors to current class stats
			for (PathStatItemActivation item : accessors) {
				currentCS.addAccessor(item.toAPI());
			}
			
			classStats.add(currentCS);
		}
		
		return classStats;
		
	}
	
	private void addToAccessors(AbstractActivation a, Collection<PathStatItemActivation> accessors) {
		//check if there already exists an accessor with same (parentClazz,parentFieldName)
		
		String parentClassName = a.getParentClass() == null ? "query" : a.getParentClass().getName();
		String parentFieldName = a.getParentFieldName();
		
		if (parentFieldName == null) parentFieldName = "query";
		
		for (PathStatItemActivation psi : accessors) {
			if (psi.match(parentClassName, parentFieldName)) {
				psi.addActivation(a);
				return;
			}
		}
		
		PathStatItemActivation newPSI = new PathStatItemActivation();
		newPSI.setcName(parentClassName);
		newPSI.setfName(parentFieldName);
		newPSI.addActivation(a);
		accessors.add(newPSI);
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
