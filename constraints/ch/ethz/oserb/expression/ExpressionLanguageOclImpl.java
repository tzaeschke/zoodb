/**
 * 
 */
package ch.ethz.oserb.expression;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.emf.common.util.URI;

import ch.ethz.oserb.example.ExamplePerson;
import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.language.ocl.resource.ocl.Ocl22Parser;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import tudresden.ocl20.pivot.tools.codegen.ocl2java.IOcl2JavaSettings;
import tudresden.ocl20.pivot.tools.codegen.ocl2java.Ocl2JavaFactory;
import net.sf.oval.exception.ExpressionEvaluationException;
import net.sf.oval.expression.ExpressionLanguage;

/**
 * @author oserb
 *
 */
public class ExpressionLanguageOclImpl implements ExpressionLanguage {
	final static File simpleModel = new File("resources/model/ch/ethz/oserb/example/ModelProviderClass.class");
	final static File simpleOclConstraints = new File("resources/constraints/examplePerson.ocl");

	/* (non-Javadoc)
	 * @see net.sf.oval.expression.ExpressionLanguage#evaluate(java.lang.String, java.util.Map)
	 */
	@Override
	public Object evaluate(String expr, Map<String, ?> values)throws ExpressionEvaluationException{
		boolean valid=true;
		try {
			// load model
			IModel model = StandaloneFacade.INSTANCE.loadJavaModel(simpleModel);
			// create empty model instance
			IModelInstance modelInstance = new JavaModelInstance(model);
			// add object to model
			modelInstance.addModelInstanceElement(values.get("_this"));
			// parse OCL constraints
			//List<Constraint> constraintList = StandaloneFacade.INSTANCE.parseOclConstraints(model, simpleOclConstraints);
			List<Constraint> constraintList = Ocl22Parser.INSTANCE.parseOclString(expr, model);
			// interpret OCL constraints
			for (IInterpretationResult result : StandaloneFacade.INSTANCE.interpretEverything(modelInstance, constraintList)) {
				/*System.out.println("  " + result.getModelObject() + " ("
						+ result.getConstraint().getKind() + ": "
						+ result.getConstraint().getSpecification().getBody()
						+ "): " + result.getResult());*/
				valid &= ((JavaOclBoolean)result.getResult()).isTrue();
			}
			
		} catch (ModelAccessException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
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
