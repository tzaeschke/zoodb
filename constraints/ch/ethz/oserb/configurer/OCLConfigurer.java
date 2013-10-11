/**
 * 
 */
package ch.ethz.oserb.configurer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;

import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import net.sf.oval.Validator;
import net.sf.oval.configuration.Configurer;
import net.sf.oval.configuration.pojo.elements.ClassConfiguration;
import net.sf.oval.configuration.pojo.elements.ConstraintSetConfiguration;
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
		/*// interpret OCL constraints
		for (IInterpretationResult result : StandaloneFacade.INSTANCE.interpretEverything(modelInstance, constraintList)) {
			valid &= ((JavaOclBoolean)result.getResult()).isTrue();
		}*/
	}
	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getClassConfiguration(java.lang.Class)
	 */
	@Override
	public ClassConfiguration getClassConfiguration(Class<?> clazz)
			throws InvalidConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getConstraintSetConfiguration(java.lang.String)
	 */
	@Override
	public ConstraintSetConfiguration getConstraintSetConfiguration(String constraintSetId) throws InvalidConfigurationException {
		
		return null;
	}

}
