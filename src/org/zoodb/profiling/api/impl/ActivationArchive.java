package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.Activation;

/**
 * Holds all activations for a single class (over all transactions)
 * @author tobias
 *
 */
public class ActivationArchive {

	private Collection<AbstractActivation> items;
	
	public ActivationArchive() {
		items = new LinkedList<AbstractActivation>();
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

}
