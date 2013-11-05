package ch.ethz.oserb.example;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.jdo.Extent;
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
	 * @throws TemplateException 
	 * @throws MalformedURLException 
	 */
    public static void main(String[] args){  	
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
			cm = new ConstraintManager(pm,modelProviderClass,oclConfigs);
		} catch (IOException e) {
			LOG.error("Could not load config: "+e.getMessage());
			throw new RuntimeException("Could not load config: "+e.getMessage());
		} catch (ModelAccessException e) {
			LOG.error("Could not load model: "+e.getMessage());
			throw new RuntimeException("Could not load model: "+e.getMessage());
		} catch (ParseException e) {
			LOG.error("Parsing ocl document ("+oclConfig.getName()+") failed:\n"+e.getMessage());
			throw new RuntimeException("Parsing ocl document ("+oclConfig.getName()+") failed:\n"+e.getMessage());
		} catch (TemplateException e) {
			LOG.error("Template Exception");
			throw new RuntimeException("Template Exception!");
		} catch (ClassNotFoundException e) {
			LOG.error("Class not found!");
			throw new RuntimeException("Class not found!");
		}   
		
		// define schemas
		cm.begin();
		ZooSchema.defineClass(cm.getPersistenceManager(), ExamplePerson.class);
		cm.commit();
		
		try{
			// begin transaction: write
			cm.begin();
			cm.disableProfile("hard");
			ExamplePerson fred = new ExamplePerson("Fred",12);
			fred.setAge(10);
			cm.makePersistent(fred);
			cm.makePersistent(new ExamplePerson("Feuerstein",18));
			ExamplePerson barney = new ExamplePerson("Barney");
			
			// immediate evaluation
			cm.validateImmediate(barney);
			cm.makePersistent(barney);
			
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
