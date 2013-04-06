package org.zoodb.profiling.simulator;

import java.util.ArrayList;
import java.util.Random;

public class ActionArchive {
	
	private ArrayList<IAction> actions;
	private ArrayList<Double> weights;
	
	private static final Random RND = new Random(0);
	
	public ActionArchive() {
		actions = new ArrayList<IAction>();
		weights = new ArrayList<Double>();
	}
	
	/**
	 * TODO: Returns the next action, given a probability distribution over all actions
	 * @return
	 */
	public IAction getNextAction() {
		int i = sampleActionByWeight();
		return actions.get(i);
	}
	
	public void addAction(IAction a, double weight) {
		actions.add(a);
		weights.add(weight);
	}
	
	/**
	 * Samples the next action according to their distribution using inverse transform sampling
	 * @return
	 */
	private int sampleActionByWeight() {
		double tmp = RND.nextDouble();
		double cumSum = 0;
		
		int actionCount = actions.size();
		
		
		for (int i=0;i<actionCount;i++) {
			cumSum += weights.get(i);
			
			if (tmp <= cumSum) {
				return i;
			}
		}
		return 0;
	}
}
