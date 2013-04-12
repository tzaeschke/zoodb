package org.zoodb.profiling.acticvity2;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model2.Author;
import org.zoodb.profiling.model2.Conference;
import org.zoodb.profiling.simulator.AbstractAction;

public class ShortcutAction extends AbstractAction {

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
//			for (Conference c : cs.getConferences()) {
//
//				//this.getLogger().info("Conference: " + c.getIssue() + " (" + c.getYear() + ")");
//				
//				c.getIssue();
//				c.getYear();
//				
//				if (c.getPublications() != null && !c.getPublications().isEmpty()) {
//					Publication p = c.getPublications().get(0);
//					
//					if (p.getTargetA() != null && !p.getTargetA().isEmpty()) {
//						Author a = p.getTargetA().iterator().next();
//						//this.getLogger().info("Keynote Speaker: " + a.getName());
//						
//						a.getName();
//					}
//				}
//			}			
//		}
//		
//		pm.currentTransaction().commit();
//	
//	}
	
	private void repeatXMaxShortcutImplemented(PersistenceManager pm,int max) {
		int count = 0;
		
		pm.currentTransaction().begin();

		Extent<Conference> conferenceSeries = pm.getExtent(Conference.class);
		
		for (Conference c : conferenceSeries) {
			count++;
			
			if (count >= max) {
				break;
			}

			c.getIssue();
			c.getYear();

			Author keynoteAuthor = c.getKeynoteAuthor();

			if (keynoteAuthor != null) {
				keynoteAuthor.getName();
			}
		}
		
		pm.currentTransaction().commit();
	
	}
	

}
