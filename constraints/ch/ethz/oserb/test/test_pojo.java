package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.ValidatorFactory;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.pojo.elements.ClassConfiguration;
import net.sf.oval.configuration.pojo.elements.MethodReturnValueConfiguration;
import net.sf.oval.constraint.LengthCheck;
import net.sf.oval.exception.ConstraintsViolatedException;
import net.sf.oval.internal.util.ReflectionUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.ConstraintManagerFactory;
import ch.ethz.oserb.test.data.Student;

public class test_pojo {
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
		oclConfigs.add(new OCLConfig(oclConfig, "oclConfigurer", 1));
		
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
	public void test() throws Exception {
		// define schemas
		cm.begin();
		PersistenceManager pm = cm.getPersistenceManager();
		ZooSchema.defineClass(pm, Student.class);
		cm.commit();
		cm.close();
		
		LengthCheck check = new LengthCheck();
		check.setMin(6);
		check.setMax(8);
		check.setProfiles("field");
		ConstraintManagerFactory.addChecks(ReflectionUtils.getField(Student.class, "name"), check);
		
		cm = ConstraintManagerFactory.getConstraintManager(ConstraintManager.CouplingMode.DEFERRED);		
		cm.disableAllProfiles();
				
		// field check
		try{
			cm.begin();
			cm.enableProfile("field");
			Student alexandra = new Student(1,"Alexandra",25);
			cm.makePersistent(alexandra);
			cm.commit();
			fail("violation expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		cm.close();
		
		check = new LengthCheck();
		check.setMin(6);
		check.setMax(8);
		check.setProfiles("method");
		ConstraintManagerFactory.addChecks(ReflectionUtils.getMethod(Student.class, "getName"), check);
		
		cm = ConstraintManagerFactory.getConstraintManager(ConstraintManager.CouplingMode.DEFERRED);
		cm.disableAllProfiles();
		
		// method return check
		try{
			cm.begin();
			cm.enableProfile("method");
			Student david = new Student(1,"David",28);
			cm.makePersistent(david);
			cm.commit();
			fail("violation expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// more checks?
	}
}
