package org.zoodb.profiling.acticvity1;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.AuthorContact;
import org.zoodb.profiling.simulator.AbstractAction;

public class AuthorMergeTest extends AbstractAction {

	public Object executeAction(PersistenceManager pm) {
		
		int max = 1000;
		
		beforeOptimized(pm,max);
		//afterOptimized(pm,max);
		
		
		return null;
	}
	
	private void beforeOptimized(PersistenceManager pm, int max) {
		pm.currentTransaction().begin();
		Extent<Author> allAuthors = pm.getExtent(Author.class);
		int i=0;
		for (Author a : allAuthors) {
			i++;
			
			if (i==max) break;
			a.getName();
			AuthorContact ad = a.getDetails();
			ad.getEmail();
			ad.getUniversity();
			
			
		}
		pm.currentTransaction().commit();
	}
	
//	private void afterOptimized(PersistenceManager pm, int max) {
//		pm.currentTransaction().begin();
//		Extent<Author> allAuthors = pm.getExtent(Author.class);
//		int i=0;
//		for (Author a : allAuthors) {
//			i++;
//			
//			if (i==max) break;
//			a.getName();
//			a.getEmail();
//			a.getUniversity();
//		}
//		pm.currentTransaction().commit();
//	}

}
