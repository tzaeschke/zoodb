/**
 * 
 */
package ch.ethz.oserb.expression;

import java.util.List;
import java.util.Map;

import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.language.ocl.resource.ocl.Ocl22Parser;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.exception.TypeNotFoundInModelException;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import net.sf.oval.exception.ExpressionEvaluationException;
import net.sf.oval.expression.ExpressionLanguage;
import net.sf.oval.internal.Log;

/**
 * @author oserb
 *
 */
public class ExpressionLanguageOclImpl implements ExpressionLanguage {
	private static final Log LOG = Log.getLog(ExpressionLanguageOclImpl.class);
	
	private IModel model;
	private IModelInstance modelInstance;
	
	// constructor
	public ExpressionLanguageOclImpl(IModel model){
		// load model
		this.model = model;
	}
	
	/* (non-Javadoc)
	 * @see net.sf.oval.expression.ExpressionLanguage#evaluate(java.lang.String, java.util.Map)
	 */
	@Override
	public Object evaluate(String expr, Map<String, ?> values)throws ExpressionEvaluationException{
		boolean valid=true;
		try {
			// create empty model instance
			modelInstance = new JavaModelInstance(model);	
			// add object to model
			modelInstance.addModelInstanceElement(values.get("_this"));
			// parse OCL constraints from annotation
			List<Constraint> constraintList = Ocl22Parser.INSTANCE.parseOclString(expr, model);
			// interpret OCL constraints
			for (IInterpretationResult result : StandaloneFacade.INSTANCE.interpretEverything(modelInstance, constraintList)) {
				valid &= ((JavaOclBoolean)result.getResult()).isTrue();
			}			
		} catch (TypeNotFoundInModelException e) {
			LOG.error("Object type not part of model!");
		} catch (ParseException e) {
			LOG.error("Parsing OCL annotation failed!");
		}
		return valid;
	}

	/* (non-Javadoc)
	 * @see net.sf.oval.expression.ExpressionLanguage#evaluateAsBoolean(java.lang.String, java.util.Map)
	 */
	@Override
	public boolean evaluateAsBoolean(String expr, Map<String, ?> values)throws ExpressionEvaluationException {
		final Object result = evaluate(expr, values);
		if (!(result instanceof Boolean))
			throw new ExpressionEvaluationException("The script must return a boolean value.");
		return (Boolean) result;
	}
}
