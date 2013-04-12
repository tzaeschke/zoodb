package org.zoodb.profiling.acticvity2;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model2.Conference;
import org.zoodb.profiling.model2.ConferenceSeries;
import org.zoodb.profiling.model2.Publication;
import org.zoodb.profiling.simulator.AbstractAction;

public class MistakeAction extends AbstractAction {

	/* 
	 * This action should detect a shortcut from conference to author
	 * 
	 * E.g. by taking the  the first author of author of the first publication 
	 * of the set of publications for a conference (--> keynote speaker)
	 * (non-Javadoc)
	 * @see ch.ethz.globis.jdo.simulator.IAction#executeAction(javax.jdo.PersistenceManager)
	 */
	public Object executeAction(PersistenceManager pm) {
		
		//task will be executed for 500 conferenceseries
		int max = 2000; 
		//int max = Integer.MAX_VALUE;
		
		//repeatXMax(pm,max);
		repeatXMaxShortcutImplemented(pm,max);
		return null;
	}

	/**
	 * We execute the same action 'max' times
	 * @param pm
	 */
//	private void repeatXMax(PersistenceManager pm,int max) {
//		int count = 0;
//		
//		pm.currentTransaction().begin();
//
//		Extent<ConferenceSeries> conferenceSeries = pm.getExtent(ConferenceSeries.class);
//		
//		for (ConferenceSeries cs : conferenceSeries) {
//			count++;
//			
//			if (count >= max) {
//				break;
//			}
//			
//			cs.getDBLPkey();
//			cs.getName();
//			
//			for (Publication p: cs.getPublications()) {
//				Conference c= p.getConference();
//				c.getIssue();
//				c.getYear();
//				c.getLocation();
//			}
//		}
//		
//		pm.currentTransaction().commit();
//	
//	}
	
	private void repeatXMaxShortcutImplemented(PersistenceManager pm,int max) {
		int count = 0;
		
		pm.currentTransaction().begin();

		Extent<ConferenceSeries> conferenceSeries = pm.getExtent(ConferenceSeries.class);
		
		for (ConferenceSeries cs : conferenceSeries) {
			count++;
			
			if (count >= max) {
				break;
			}
			
			cs.getDBLPkey();
			cs.getName();
			
			for (Conference c: cs.getConferences()) {
				c.getIssue();
				c.getYear();
				c.getLocation();
			}
		}
		
		pm.currentTransaction().commit();
	
	}
	

}
