package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashSet;

import org.zoodb.profiling.api.Activation;

/**
 * Holds all activations for a single class (over all transactions)
 * @author tobias
 *
 */
public class ActivationArchive {

	private Collection<Activation> items;
	
	public ActivationArchive() {
		items = new HashSet<Activation>();
	}
	
	public void addItem(Activation a) {
		items.add(a);
	}

}
