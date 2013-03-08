package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;

import ch.ethz.globis.profiling.commons.statistics.PathStatItem;

public class PathStatItemActivation extends PathStatItem {
	
	private String cName;
	private String fName;
	private int frequency;
	
	private Collection<PathStatItemActivation> predecessors = new LinkedList<PathStatItemActivation>();

	
	public void addActivation(AbstractActivation a) {
		frequency++;
		mergeChildren(a);
	}
	
	/**
	 * Merges the children of 'a' into this objects predecessors
	 * @param a
	 */
	private void mergeChildren(AbstractActivation aa) {
		
		AbstractActivation a = aa.getParent();
		
		if (a != null) {
			String parentClassName = a.getParentClass() == null ? "query" : a.getParentClass().getName();
			String parentFieldName = a.getParentFieldName();
				
			if (parentFieldName == null) parentFieldName = "query";

			boolean inserted = false;
			for (PathStatItemActivation psi : predecessors) {
				if (psi.match(parentClassName, parentFieldName)) {
					psi.addActivation(a);
					inserted = true;
					break;

				}
			}
			if (!inserted) {
				PathStatItemActivation newPSI = new PathStatItemActivation();
				newPSI.setcName(parentClassName);
				newPSI.setfName(parentFieldName);
				newPSI.addActivation(a);
				predecessors.add(newPSI);
			}
		}		
	}

	
	public boolean match(String othercName, String otherfName) {
		return othercName.equals(this.cName) && otherfName.equals(this.fName);
	}

	public String getcName() {
		return cName;
	}
	public void setcName(String cName) {
		this.cName = cName;
	}
	public String getfName() {
		return fName;
	}
	public void setfName(String fName) {
		this.fName = fName;
	}
	public int getFrequency() {
		return frequency;
	}
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	
	public void prettyPrint(int i) {
		System.out.println();
		String prefix = "";
		for (int l=0;l<i;l++) {
			System.out.print("\t");
		}
		System.out.print(cName + ":" + fName + ":" + frequency);
		for (PathStatItemActivation p : predecessors) {
			p.prettyPrint(++i);
		} 
	}
	
	public PathStatItem toAPI() {
		PathStatItem me = new PathStatItem();
		me.setFrequency(this.frequency);
		me.setfName(this.fName);
		me.setcName(this.cName);
		
		Collection<PathStatItem> accessors = new LinkedList<PathStatItem>();
		
		for (PathStatItemActivation p : predecessors) {
			accessors.add(p.toAPI());
		}
		
		me.setPredecessors(accessors);
		return me;
		
	}
	
	
}
