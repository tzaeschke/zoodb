package org.zoodb.profiling.pop2;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.profiling.model2.Author;
import org.zoodb.profiling.model2.Publication;

public class AggregationPopulator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PersistenceManager pm = openDB("dblp");
		


		
		Extent<Author> authors = pm.getExtent(Author.class);
		pm.currentTransaction().begin();
		int i=0;
		
		for (Author a : authors) {
			//if (i==0) {
			//	pm.currentTransaction().begin();
			//}
			
			int aggregatedRating = 0;
			
			if (a.getSourceA() != null && !a.getSourceA().isEmpty()) {
				//simulate aggregation
				for (Publication p : a.getSourceA()) {
					aggregatedRating += p.getRating();
				}
			}
			
			a.setAggregatedRating(aggregatedRating);
			i++;
			
//			if (i==20000) {
//				System.out.println("start commit");
//				pm.currentTransaction().commit();
//				i=0;
//			}
		}
		System.out.println("before final commit");
		//if (i<20000) {
			pm.currentTransaction().commit();
		//}
		System.out.println("before closing db");
		
		closeDB(pm);

	}
	
    private static PersistenceManager openDB(String dbName) {
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        return pm;
    }
    
    private static void closeDB(PersistenceManager pm) {
        if (pm.currentTransaction().isActive()) {
            pm.currentTransaction().rollback();
        }
        pm.close();
        pm.getPersistenceManagerFactory().close();
    }

}
