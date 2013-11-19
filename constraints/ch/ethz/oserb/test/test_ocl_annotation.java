package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

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

public class test_ocl_annotation {
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
		oclConfigs.add(new OCLConfig(oclConfig, "oclConfigurer", 0));
		
		ValidatorFactory vf = new ValidatorFactory(modelProviderClass,oclConfigs);
		ConstraintManagerFactory.initialize(pmf,vf);
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
		cm.disableAllProfiles();
		cm.enableProfile("oclAnnotation");
		cm.commit();
		
		// baseline
		try{
			cm.begin();
			Student alice = new Student(1,"Alice",19);
			cm.makePersistent(alice);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 0);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// annotated ocl constraint failing
		try{
			cm.begin();
			Student bob = new Student(1,"Bob",17);
			cm.makePersistent(bob);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
	}

}
