package ch.ethz.oserb.example;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.exception.ConstraintsViolatedException;
import net.sf.oval.internal.Log;

import org.zoodb.jdo.api.ZooJdoHelper;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;
import ch.ethz.oserb.ConstraintManager;


/**
 * @author oserb
 *
 */
public class Example {
	
	private static final Log LOG = Log.getLog(Example.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 * @throws TemplateException 
	 * @throws MalformedURLException 
	 */
    public static void main(String[] args) throws Exception{  	
        String dbName = "ExampleDB";
        createDB(dbName);       
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        
        // set up constraint manager        
		File modelProviderClass = new File("resources/model/ch/ethz/oserb/example/ModelProviderClass.class");
		File oclConfig = new File("resources/constraints/constraints.ocl");
		File xmlConfig = new File("resources/constraints/constraints.xml");

		ConstraintManager cm;
		try {
			// defining OCL configuration
			ArrayList<OCLConfig> oclConfigs = new ArrayList<OCLConfig>();
			oclConfigs.add(new OCLConfig(oclConfig, "hard", 0));
			oclConfigs.add(new OCLConfig(oclConfig, "soft", 1));
			
			// get constraintManager
			ConstraintManager.initialize(pm,modelProviderClass,oclConfigs);
			cm = ConstraintManager.getInstance();
		}catch (Exception e){
			LOG.error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
		
		// define schemas
		cm.begin();
		ZooSchema.defineClass(cm.getPersistenceManager(), ExamplePerson.class);
		cm.commit();
		
		try{
			// begin transaction: write
			cm.begin();
			cm.disableProfile("hard");
			cm.makePersistent(new ExamplePerson("Fred",12,1));
			cm.makePersistent(new ExamplePerson("Feuerstein",18,2));
			cm.makePersistent(new ExamplePerson("Barney",22,1));
			
			// deferred validation
			cm.commit();
		}catch (ConstraintsViolatedException e) {
			boolean abort = false;
			// get violated asserts
			for(ConstraintViolation constraintViolation : e.getConstraintViolations()){
				LOG.error(constraintViolation.getMessage());
			}
			if(abort){
				cm.abort();
			}else{
				cm.currentTransaction().commit();
			}
		}finally{
			assert(!cm.currentTransaction().isActive());
		}
		
		try{
			// begin transaction: read
			cm.begin();
			cm.disableProfile("soft");
			cm.enableProfile("hard");
			Extent<ExamplePerson> ext = cm.getExtent(ExamplePerson.class);
			Iterator<ExamplePerson> iter = ext.iterator();
			while(iter.hasNext()){
				ExamplePerson p = (ExamplePerson) iter.next();
				// mark as dirty to show 
				p.jdoZooMarkDirty();
				System.out.println("Person found: " + p.getName()+", "+p.getAge());
			}       
			ext.closeAll();     
			cm.commit();
		}catch (ConstraintsViolatedException e) {
			boolean abort = false;
			// get violated asserts
			for(ConstraintViolation constraintViolation : e.getConstraintViolations()){
				LOG.error(constraintViolation.getMessage());
			}
			if(abort){
				cm.abort();
			}else{
				cm.currentTransaction().commit();
			}
		}finally{
			assert(!cm.currentTransaction().isActive());
		}        
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
