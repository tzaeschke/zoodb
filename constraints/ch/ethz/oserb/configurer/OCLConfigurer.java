/**
 * 
 */
package ch.ethz.oserb.configurer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.ethz.oserb.example.ExamplePerson;
import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import net.sf.oval.Check;
import net.sf.oval.Validator;
import net.sf.oval.configuration.Configurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.pojo.elements.ClassConfiguration;
import net.sf.oval.configuration.pojo.elements.ConstraintSetConfiguration;
import net.sf.oval.configuration.pojo.elements.MethodConfiguration;
import net.sf.oval.configuration.pojo.elements.ObjectConfiguration;
import net.sf.oval.constraint.Assert;
import net.sf.oval.constraint.AssertCheck;
import net.sf.oval.exception.InvalidConfigurationException;
import net.sf.oval.internal.Log;

/**
 * @author oserb
 *
 */
public class OCLConfigurer implements Configurer, Serializable {
	
	private IModel model;
	private IModelInstance modelInstance;
	private File oclFile;
	private List<Constraint> constraintList;
	private POJOConfigurer pojoConfigurer = new POJOConfigurer();
	private static final Log LOG = Log.getLog(Validator.class);
	
	public OCLConfigurer(File oclFile, IModel model){
		LOG.info("OCL configurer registered: {1}", this.getClass().getName());
		this.oclFile = oclFile;
		this.model = model;
		
		// create empty model instance
		modelInstance = new JavaModelInstance(model);	
		// parse OCL constraints from document
		try {
			constraintList = StandaloneFacade.INSTANCE.parseOclConstraints(model, oclFile);
		} catch (IOException e) {
			LOG.error("Could not load ocl file!");
		} catch (ParseException e) {
			LOG.error("Parsing ocl document ("+oclFile.getName()+") failed:\n"+e.getMessage());
		}
		
		// map OCL to Oval POJO
		Set<ClassConfiguration> classConfigurations = new HashSet<ClassConfiguration>();
		pojoConfigurer.setClassConfigurations(classConfigurations);
		for(Constraint constraint: constraintList){
			// extract class name
			Pattern pattern = Pattern.compile("name=(.*?),namespace");
			Matcher matcher = pattern.matcher(constraint.getConstrainedElement().toString());
			matcher.find();
			
			// define assert expression
			AssertCheck assertCheck = new AssertCheck();
			assertCheck.setExpr("context "+matcher.group(1)+" "+constraint.getSpecification().getBody().replaceAll("[\n\t\r]", ""));
			assertCheck.setLang("ocl");
			
			// define class configuration			
			ClassConfiguration classConfiguration = new ClassConfiguration();
			classConfigurations.add(classConfiguration);
			// TODO: proper initialization
			classConfiguration.type = ExamplePerson.class;
			
			// define object configuration
			ObjectConfiguration objectConfiguration = new ObjectConfiguration();
			classConfiguration.objectConfiguration = objectConfiguration;
			objectConfiguration.checks = new ArrayList<Check>();
			objectConfiguration.checks.add(assertCheck);
		}
	}
	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getClassConfiguration(java.lang.Class)
	 */
	@Override
	public ClassConfiguration getClassConfiguration(Class<?> clazz)	throws InvalidConfigurationException {
		
		return pojoConfigurer.getClassConfiguration(clazz);
	}

	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getConstraintSetConfiguration(java.lang.String)
	 */
	@Override
	public ConstraintSetConfiguration getConstraintSetConfiguration(String constraintSetId) throws InvalidConfigurationException {
		
		return pojoConfigurer.getConstraintSetConfiguration(constraintSetId);
	}

}
