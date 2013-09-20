/**
 * 
 */
package ch.ethz.oserb;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;

import org.zoodb.jdo.api.ZooJdoHelper;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.ex1.ExamplePerson;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.constraints.ConstraintManager;


/**
 * @author oserb
 *
 */
public class Example {
	private static PersistenceManager pm;
	
	private static class ListenerCreate implements CreateLifecycleListener {
		@Override
		public void postCreate(InstanceLifecycleEvent arg0) {	
			System.out.println("Listener");
		}
	}
		
	/**
	 * @param args
	 */
    public static void main(String[] args) {  	
        String dbName = "ExampleDB";
        createDB(dbName);       
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        
        // define class
        pm.currentTransaction().begin();
        ZooSchema.defineClass(pm, ExamplePerson.class);
        pm.currentTransaction().commit();
        
        // set up constraint manager
        ConstraintManager cm = new ConstraintManager(pm);
        
        // register listener
        pm.addInstanceLifecycleListener(new ListenerCreate(), ExamplePerson.class);
        
        // begin transaction: write
        pm.currentTransaction().begin();
        pm.makePersistent(new ExamplePerson("Fred"));
        pm.currentTransaction().commit();
        
        // begin transaction: write
        pm.currentTransaction().begin();
        pm.makePersistent(new ExamplePerson("Feuerstein"));
        pm.makePersistent(new ExamplePerson("Barney"));
        pm.currentTransaction().commit();
        
        // begin transaction: read
        pm.currentTransaction().begin();
        Extent<ExamplePerson> ext = pm.getExtent(ExamplePerson.class);
        Iterator iter = ext.iterator();
        while(iter.hasNext()){
        	ExamplePerson p = (ExamplePerson) iter.next();
            System.out.println("Person found: " + p.getName());
        }        
        ext.closeAll();                
        pm.currentTransaction().commit();
        
        closeDB(pm);
    }
          
    /**
     * Create a database.
     * 
     * @param dbName Name of the database to create.
     */
    private static void createDB(String dbName) {
        // remove database if it exists
        if (ZooHelper.dbExists(dbName)) {
            ZooHelper.removeDb(dbName);
        }

        // create database
        // By default, all database files will be created in %USER_HOME%/zoodb
        ZooHelper.createDb(dbName);
    }

    
    /**
     * Close the database connection.
     * 
     * @param pm The current PersistenceManager.
     */
    private static void closeDB(PersistenceManager pm) {
        if (pm.currentTransaction().isActive()) {
            pm.currentTransaction().rollback();
        }
        pm.close();
        pm.getPersistenceManagerFactory().close();
    }

}
