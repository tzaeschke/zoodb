package org.zoodb.profiling.acticvity1;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model1.Publication;
import org.zoodb.profiling.simulator.AbstractAction;

public class SplitAction extends AbstractAction {

	/* 
	 * To enforce a split class suggestions, we need a class which is big enough and has enough attributes --> publication
	 * (non-Javadoc)
	 * @see ch.ethz.globis.jdo.simulator.IAction#executeAction(javax.jdo.PersistenceManager)
	 */
	public Object executeAction(PersistenceManager pm) {
		//DBLPQueries queries = new DBLPQueries(pm);
		
		int max = 100000;
		
		beforeOptimized(pm,max);
		//afterOptimized(pm,max);
		return null;
	}

//	private void afterOptimized(PersistenceManager pm, int max) {
//		pm.currentTransaction().begin();
//		
//		Extent<Publication> publications = pm.getExtent(Publication.class);
//		
//		int i=0;
//		
//		for (Publication p : publications) {
//			i++;
//			
//			if (i==max) {
//				break;
//			}
//			p.getTitle();
//			p.getYear();
//			PublicationSplit ps = p.getPs();
//			ps.setCitationCount(1);
//			ps.setDownloadCount(1);
//		}
//		
//		pm.currentTransaction().commit();
//	}

	
	private void beforeOptimized(PersistenceManager pm, int max) {
		pm.currentTransaction().begin();
		
		Extent<Publication> publications = pm.getExtent(Publication.class);
		
		int i=0;
		
		for (Publication p : publications) {
			i++;
			
			if (i==max) {
				break;
			}
			p.getTitle();
			p.getYear();
			p.getConference();
			p.getTargetA();
			p.getKey();
			p.getRating();
			p.getTargetT();
			p.getAbstract();
			
			p.setCitationCount(1);
			p.setViewCount(1);
			p.setDownloadCount(1);
//			p.getCitationCount();
//			p.getViewCount();
//			p.getDownloadCount();
		}
		
		pm.currentTransaction().commit();
	}

	

}
