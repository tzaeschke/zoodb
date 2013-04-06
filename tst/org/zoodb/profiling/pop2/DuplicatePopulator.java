package org.zoodb.profiling.pop2;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.profiling.model2.Publication;

public class DuplicatePopulator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PersistenceManager pm = openDB(args[0]);
		
		pm.currentTransaction().begin();
		
		Extent<Publication> publications = pm.getExtent(Publication.class);
		boolean started = true;
		int i=0;
		
		for (Publication p : publications) {
			if (i==0 && !started) {
				pm.currentTransaction().begin();
			}
			String conferenceIssue = null; 
			
			if (p.getConference() != null) {
				conferenceIssue = p.getConference().getIssue();
			}
			
			p.setConferenceIssue(conferenceIssue);
			
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
