package org.zoodb.profiling.acticvity1;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.Publication;
import org.zoodb.profiling.simulator.IAction;

public class AggregationAction implements IAction {

	public Object executeAction(PersistenceManager pm) {
		
		int max = 10000;
		
		//useStoredAggregatedValue(pm,max);
		computeValue(pm,max);
		
		return null;
	}
	
	
	private void computeValue(PersistenceManager pm, int max) {
		pm.currentTransaction().begin();
		Extent<Author> authors = pm.getExtent(Author.class);
		
		int i=0;
		for (Author a : authors) {
			i++;
			if (i==max) break;
			for (Publication p : a.getSourceA()) {
				p.getRating();
			}
		}
		pm.currentTransaction().commit();
		
	}


//	private void useStoredAggregatedValue(PersistenceManager pm, int max) {
//		pm.currentTransaction().begin();
//		Extent<Author> authors = pm.getExtent(Author.class);
//		
//		int i=0;
//		for (Author a : authors) {
//			i++;
//			if (i==max) break;
//			a.getAggregatedRating();
//		}
//		pm.currentTransaction().commit();
//	}

}
