package ch.ethz.oserb.example;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.exception.ConstraintsViolatedException;
import net.sf.oval.internal.Log;

import org.zoodb.jdo.api.ZooJdoHelper;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;
import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.exception.ConstraintException;
import ch.ethz.oserb.violation.Violation;
import ch.ethz.oserb.violation.Violation.Severity;


/**
 * @author oserb
 *
 */
public class Example {
	
	private static PersistenceManager pm;
	private static final Log LOG = Log.getLog(Example.class);
	
	/**
	 * @param args
	 * @throws TemplateException 
	 * @throws MalformedURLException 
	 */
    public static void main(String[] args) throws MalformedURLException, TemplateException {  	
        String dbName = "ExampleDB";
        createDB(dbName);       
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        
        // set up constraint manager        
		File modelProviderClass = new File("resources/model/ch/ethz/oserb/example/ModelProviderClass.class");
		File oclConfig = new File("resources/constraints/constraints.ocl");
		File xmlConfig = new File("resources/constraints/constraints.xml");

        try {
			ConstraintManager cm = new ConstraintManager(pm, oclConfig,modelProviderClass,Severity.IGNORE);
			
			try{
				// define class
				cm.begin();
				ZooSchema.defineClass(cm.getPersistenceManager(), ExamplePerson.class);
				cm.commit();
			}finally{
				assert(!cm.currentTransaction().isActive());
			}
			
			try{
				// begin transaction: write
				System.out.println("write:");
				cm.begin();
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
				for(ConstraintViolation constraintViolation : e.getConstraintViolations()){
					System.out.println(constraintViolation.getMessage());
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
				System.out.println("read:");
				cm.begin();
				Extent<ExamplePerson> ext = cm.getExtent(ExamplePerson.class);
				Iterator iter = ext.iterator();
				while(iter.hasNext()){
					ExamplePerson p = (ExamplePerson) iter.next();
					System.out.println("Person found: " + p.getName()+", "+p.getAge());
				}        
				ext.closeAll();                
				cm.commit();
			}catch (ConstraintsViolatedException e) {
				boolean abort = false;
				/*for(Violation violation:e.getViolations()){
					System.out.println("constraint violation: "+violation.getConstraint());
					abort |= (violation.getSeverity()==Severity.ERROR);
				}*/
				for(ConstraintViolation constraintViolation : e.getConstraintViolations()){
					StringBuilder msg = new StringBuilder();
					msg.append(constraintViolation.getMessage());
					msg.append("\nViolations:");
					//LOG.error(constraintViolation.getMessage());
					for(String violation:cm.getCause(constraintViolation)){
						msg.append("\n\t");
						msg.append(violation.trim());
						//LOG.error(violation);
					}
					LOG.error(msg.toString());
				}
				if(abort){
					cm.abort();
				}else{
					cm.currentTransaction().commit();
				}
			}finally{
				assert(!cm.currentTransaction().isActive());
			}
		} catch (IOException e) {
			LOG.error("Could not load config: "+e.getMessage());
		} catch (ModelAccessException e) {
			LOG.error("Could not load model: "+e.getMessage());
		} catch (ParseException e) {
			LOG.error("Parsing ocl document ("+oclConfig.getName()+") failed:\n"+e.getMessage());
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
