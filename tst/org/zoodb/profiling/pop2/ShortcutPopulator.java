package org.zoodb.profiling.pop2;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.profiling.model2.Author;
import org.zoodb.profiling.model2.Conference;
import org.zoodb.profiling.model2.Publication;

public class ShortcutPopulator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Populating shortcuts ...");
		PersistenceManager pm = openDB(args[0]);
		
		pm.currentTransaction().begin();
		
		Extent<Conference> conferences = pm.getExtent(Conference.class);
		boolean started = true;
		int i=0;
		
		for (Conference c : conferences) {
			if (i==0 && !started) {
				pm.currentTransaction().begin();
			}
			Author keynoteAuthor = null; 
			
			if (c.getPublications() != null && !c.getPublications().isEmpty()) {
				Publication p = c.getPublications().get(0);
				
				if (p.getTargetA() != null && !p.getTargetA().isEmpty()) {
					keynoteAuthor = p.getTargetA().iterator().next();
				}
			}
			
			
			if (true) throw new UnsupportedOperationException();
			//c.setKeynoteAuthor(keynoteAuthor);
			
			i++;
			
			if (i==20000) {
				pm.currentTransaction().commit();
				started = false;
				i=0;
			}
		}
		if (i<20000) pm.currentTransaction().commit();
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
