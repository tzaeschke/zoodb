package org.zoodb.profiling.api.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

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
		//this.items = new LinkedList<AbstractActivation>();
		//LinkedList nodes are too expensive!
		items = new ArrayList<AbstractActivation>();
	}
	
	public void addItem(AbstractActivation a) {
		items.add(a);
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
	
	public int getActivationCountForTrx(Trx trx) {
		int count = 0;
		for (AbstractActivation a : items) {
			if (a.getTrx() == trx) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Removes all activations which occurredn in transaction 'trx' (e.g. for rollback).
	 * Returns the number of items removed
	 * @param trx
	 * @return
	 */
	public int removeAllForTrx(Trx trx) {
		int count = 0;
		
		Iterator<AbstractActivation> iter = items.iterator();
		AbstractActivation current = null;
		
		while (iter.hasNext()) {
			current = iter.next();
			
			if (current.getTrx() == trx) {
				iter.remove();
				count++;
			}
		}		
		return count;
	}
}
