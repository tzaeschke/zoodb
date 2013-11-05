package net.sf.oval.constraint;

import static net.sf.oval.Validator.getCollectionFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AbstractAnnotationCheck;
import net.sf.oval.context.OValContext;
import net.sf.oval.exception.ExpressionEvaluationException;
import net.sf.oval.exception.ExpressionLanguageNotAvailableException;
import net.sf.oval.exception.OValException;
import net.sf.oval.expression.ExpressionLanguage;
import net.sf.oval.internal.util.ReflectionUtils;

public class OclConstraintCheck  extends AbstractAnnotationCheck<OclConstraint>{
	
	private String expr;
	private String[] profiles;
	private String message = "net.sf.oval.constraint.Assert.violated";
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6816047469276884271L;
	/**
	 * {@inheritDoc}
	 */
	public void configure(final OclConstraint constraintAnnotation)
	{
		super.configure(constraintAnnotation);
		setExpr(constraintAnnotation.expr());
		setProfiles(constraintAnnotation.profiles());
		setMessage(constraintAnnotation.message());
	}
	
	@Override
	public boolean isSatisfied(Object validatedObject, Object valueToValidate, OValContext context, Validator validator) throws OValException {
		final Object result = evaluate(validatedObject, valueToValidate, context, validator);
		return ((List<String>)result).size()==0;
	}
	/**
	 * similar to isSatisfied but does not discard causes
	 */
	public Object evaluate(final Object validatedObject, final Object valueToValidate, final OValContext context,
			final Validator validator) throws ExpressionEvaluationException, ExpressionLanguageNotAvailableException
	{
		final Map<String, Object> values = getCollectionFactory().createMap();
		values.put("_value", valueToValidate);
		values.put("_this", validatedObject);

		final ExpressionLanguage el = validator.getExpressionLanguageRegistry().getExpressionLanguage("ocl");
		return el.evaluate(expr, values);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, String> createMessageVariables()
	{
		final Map<String, String> messageVariables = getCollectionFactory().createMap(2);
		messageVariables.put("expression", expr);
		messageVariables.put("language", "ocl");
		return messageVariables;
	}
	
	public void setExpr(String expr){
		this.expr = expr;
		requireMessageVariablesRecreation();
	}
	
	public String getExpr(){
		return expr;
	}
		
	public String[] getProfiles(){
		return profiles;
	}
		
	public String getMessage()
	{
		return message;
	}
	
}
