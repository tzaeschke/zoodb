package net.sf.oval.constraint;

import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AbstractAnnotationCheck;
import net.sf.oval.context.OValContext;
import net.sf.oval.exception.OValException;

public class OclConstraintCheck  extends AbstractAnnotationCheck<OclConstraint>{

	@Override
	public boolean isSatisfied(Object validatedObject, Object valueToValidate,
			OValContext context, Validator validator) throws OValException {
		// TODO Auto-generated method stub
		System.out.println("test");
		return false;
	}
	public Object evaluate(Object validatedObject, Object valueToValidate, OValContext context, Validator validator){
		return null;
		//TODO:
	}
}
