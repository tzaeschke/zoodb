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
		PersistenceManager pm = openDB(args[0]);
		
		pm.currentTransaction().begin();
		Extent<Author> authors = pm.getExtent(Author.class);
		for (Author a : authors) {
			
			int aggregatedRating = 0;
			
			if (a.getSourceA() != null && !a.getSourceA().isEmpty()) {
				//simulate aggregation
				for (Publication p : a.getSourceA()) {
					aggregatedRating += p.getRating();
				}
			}
			
			a.setAggregatedRating(aggregatedRating);
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
