package org.zoodb.profiling.acticvity2;

import javax.jdo.PersistenceManager;

import org.zoodb.profiling.simulator.AbstractAction;

public class DuplicateAction extends AbstractAction {

	public Object executeAction(PersistenceManager pm) {
		//DBLPQueries queries = new DBLPQueries(pm);
		
		int max = 500;
		
		//beforeOptimized(pm,max);
		afterOptimized(pm,max);
		
		return null;
	}

//	private void beforeOptimized(PersistenceManager pm, int max) {
//		pm.currentTransaction().begin();
//		Extent<Author> allAuthors = pm.getExtent(Author.class);
//		int i=0;
//		for (Author a : allAuthors) {
//			i++;
//			
//			if (i==max) break;
//			a.getName();
//			
//			for (Publication p : a.getSourceA()) {
//				
//							
//				if (p.getConference() != null) {
//					p.getConference().getIssue();
//				} 
//			}
//		}
//		pm.currentTransaction().commit();
//	}
	
	private void afterOptimized(PersistenceManager pm, int max) {
		throw new UnsupportedOperationException();
//		pm.currentTransaction().begin();
//		Extent<Author> allAuthors = pm.getExtent(Author.class);
//		int i=0;
//		for (Author a : allAuthors) {
//			i++;
//			
//			if (i==max) break;
//			a.getName();
//			for (Publication p : a.getSourceA()) {
//				p.getConferenceIssue();
//			}
//		}
//		pm.currentTransaction().commit();
	}

}
