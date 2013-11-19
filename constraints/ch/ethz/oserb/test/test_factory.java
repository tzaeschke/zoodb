package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.Check;
import net.sf.oval.ValidatorFactory;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.exception.ConstraintsViolatedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.ConstraintManagerFactory;
import ch.ethz.oserb.test.data.Student;

public class test_factory {
	private ConstraintManager cm;
	
	@Before
	public void setUp() throws Exception {
	    String dbName = "ExampleDB";
	    
        // remove database if it exists
        if (ZooHelper.dbExists(dbName)) {
            ZooHelper.removeDb(dbName);
        }

        // create database (by default, all database files will be created in %USER_HOME%/zoodb)
        ZooHelper.createDb(dbName);
        
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
			
		// get constraintManager
		File modelProviderClass = new File("resources/model/ch/ethz/oserb/test/data/ModelProviderClass.class");
		File oclConfig = new File("resources/constraints/test.ocl");
		ArrayList<OCLConfig> oclConfigs = new ArrayList<OCLConfig>();
		oclConfigs.add(new OCLConfig(oclConfig, "oclConfigurerHard", 1));
		
		ValidatorFactory vf = new ValidatorFactory(modelProviderClass,oclConfigs);
		ConstraintManagerFactory.initialize(pmf,vf);
		
		for(Check check:ConstraintManagerFactory.getChecks(Student.class)){
			ConstraintManagerFactory.removeChecks(Student.class, check);
		}
		
		cm = ConstraintManagerFactory.getConstraintManager(ConstraintManager.CouplingMode.DEFERRED);
	}

	@After
	public void tearDown() throws Exception {
        if (cm.currentTransaction().isActive()) {
            cm.currentTransaction().rollback();
        }
        cm.close();
        cm.getPersistenceManagerFactory().close();
	}
	
	@Test
	public void test() {
		// define schemas
		cm.begin();
		PersistenceManager pm = cm.getPersistenceManager();
		ZooSchema.defineClass(pm, Student.class);
		cm.commit();
		
		// no errors expected instead of 10, as all removed at reference validator.
		try{
			cm.begin();
			Student alice = new Student(1,"Alice",17);
			cm.makePersistent(alice);
			Student bob = new Student(1,"Bob",15);
			cm.makePersistent(bob);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 0);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
	}

}
