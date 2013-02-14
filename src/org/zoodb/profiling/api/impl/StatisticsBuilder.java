package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IFieldAccess;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.Utils;

import ch.ethz.globis.profiling.commons.statistics.AccessGroup;
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
			currentCS.setFieldStatistics(getFieldStatisticsForClass(currentClass));
			
			long avgSize = Math.round(ProfilingManager.getInstance().getClassSizeManager().getClassStats(currentClass).getAvgClassSize());
			currentCS.setAvgSize(avgSize);
			
			Collection<AccessGroup> accessGroups = new LinkedList<AccessGroup>();
			
			Iterator<AbstractActivation> ai = aa.getIterator();
			
			while (ai.hasNext()) {
				AbstractActivation current = ai.next();
				addToAccessors(current,accessors);
				addToAccessGroups(current.getFas().values(), aa.getZooClassDef(), accessGroups);
			}
			//attach accessGroup
			currentCS.setAccessGroups(accessGroups);
			
			//attache accessors to current class stats
			for (PathStatItemActivation item : accessors) {
				currentCS.addAccessor(item.toAPI());
			}
			classStats.add(currentCS);
		}
		
		return classStats;
	}
	
	
	private void addToAccessGroups(Collection<SimpleFieldAccess> fas,ZooClassDef ccdef, Collection<AccessGroup> accessGroups) {
		//map idx to name
		Collection<String> fNames = new LinkedList<String>();
		for (SimpleFieldAccess sfa : fas) {
			fNames.add(Utils.getFieldNameForIndex(sfa.getIdx(), ccdef));
		}
		
		//check if this accessGroup already exists
		
		for (AccessGroup ag : accessGroups) {
			if (ag.matchesGroup(fNames)) {
				//update this group
				for (SimpleFieldAccess sfa : fas)  {
					String name = Utils.getFieldNameForIndex(sfa.getIdx(), ccdef);
					ag.update(name, sfa.getrCount(), sfa.getwCount());
				}
				ag.setPatternCount(ag.getPatternCount()+1);
				return;
			}
		}
		
		/*
		 * at this point we have not found a group, create a new one and init with 'fas'
		 */
		Collection<FieldStatistics> fasAsFS = new LinkedList<FieldStatistics>();
		
		for (SimpleFieldAccess sfa : fas)  {
			FieldStatistics fs = new FieldStatistics();
			fs.setFieldName(Utils.getFieldNameForIndex(sfa.getIdx(), ccdef));
			fs.setTotalReads(sfa.getrCount());
			fs.setTotalWrites(sfa.getwCount());
			
			fasAsFS.add(fs);
		}
		
		AccessGroup ag = new AccessGroup(fasAsFS);
		accessGroups.add(ag);
		
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
			long avgSize = Math.round(css.getAvgFieldSizeForField(s));
			currentFS.setAvgSize(avgSize);
			
			fs.add(currentFS);
		}		
		
		return fs;
	}
	
	private int[] getFieldData(Class<?> c, String field) {
		IFieldManager fm = ProfilingManager.getInstance().getFieldManager();
		return fm.getRWCount(c, field);
	}

}
