package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.profiling.api.AbstractActivation;

/**
 * Holds all activations for a single class (over all transactions)
 */
public class ActivationArchive {

	private Collection<AbstractActivation> items;
	
	private ZooClassDef classDef;
	
	public ActivationArchive(ZooClassDef classDef) {
		this.classDef = classDef;
		this.items = new LinkedList<AbstractActivation>();
	}
	
	public void addItem(AbstractActivation a) {
		items.add(a);
	}
	
	
	public AbstractActivation get(long oid, String trx) {
		AbstractActivation result = null;
		
		for (AbstractActivation a : items) {
			if (a.getOid() == oid && a.getTrx().equals(trx)) {
				result = a;
				break;
			}
		}
		
		return result;
	}
	
	public Iterator<AbstractActivation> getIterator() {
		return items.iterator();
	}
	
	public int size() {
		return items.size();
	}
	
	public ZooClassDef getZooClassDef() {
		return classDef;
	}
	

	public int getActivationCountByTrx(Collection<String> trxIds) {
		int result = 0;
		
		for (AbstractActivation aa : items) {
			if (trxIds.contains(aa.getTrx())) {
				result++;
			}
		}
		return result;
	}
	
	/**
	 * Returns the avg size of objects for which activations are in this archive
	 * @return
	 */
	public double getAvgObjectSize() {
		double totalBytes = 0;
		int size = 0;
		
		for (AbstractActivation a : items) {
			totalBytes += a.getBytes();
			size++;
		}
		return totalBytes / (double) size;
	}

}