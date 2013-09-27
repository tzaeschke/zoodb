package ch.ethz.oserb.example;

import java.io.File;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import net.sf.oval.exception.ConstraintsViolatedException;

import org.zoodb.jdo.api.ZooJdoHelper;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.ex1.ExamplePerson;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;


/**
 * @author oserb
 *
 */
public class Example {
	private static PersistenceManager pm;
			
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
        cm.initialize(new File("constraints/ch/ethz/oserb/example/example.xml"));
        
        // begin transaction: write
        pm.currentTransaction().begin();
        System.out.println("Person: Fred"); 
        ExamplePerson fred = new ExamplePerson("Fred",12);
        try{
        	fred.setAge(10);
        }catch (ConstraintsViolatedException e){
        	System.out.println("violation!");
        }
        pm.makePersistent(fred);
        pm.currentTransaction().commit();
        
        // begin transaction: write
        pm.currentTransaction().begin();
        System.out.println("Person: Feuerstein"); 
        try{
        	pm.makePersistent(new ExamplePerson("Feuerstein",18));
        }catch (ConstraintsViolatedException e){
        	System.out.println("violation!");
        }	
        System.out.println("Person: Barney"); 
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
