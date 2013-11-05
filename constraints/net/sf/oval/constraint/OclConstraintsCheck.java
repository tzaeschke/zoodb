package net.sf.oval.constraint;

import java.util.ArrayList;
import java.util.List;

import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AbstractAnnotationCheck;
import net.sf.oval.context.OValContext;

public class OclConstraintsCheck extends AbstractAnnotationCheck<OclConstraints> {

	public List<OclConstraintCheck> checks = new ArrayList<OclConstraintCheck>();
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6434970365024804378L;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(final OclConstraints constraintAnnotation)
	{
		super.configure(constraintAnnotation);
		setOclConstraintChecks(constraintAnnotation.value());
	}
	
	public void setOclConstraintChecks(OclConstraint[] constraints){
		for(OclConstraint constraintAnnotation:constraints){
			OclConstraintCheck oclConstraintCheck = new OclConstraintCheck();
			oclConstraintCheck.configure(constraintAnnotation);
			checks.add(oclConstraintCheck);
		}
	}
	
	public List<OclConstraintCheck> getOclConstraintChecks(){
		return checks;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getErrorCode() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMessage() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getSeverity() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 *  <b>This method is not used.</b><br>
	 *  The validation of this special constraint is directly performed by the Validator class
	 *  @throws UnsupportedOperationException always thrown if this method is invoked
	 */
	public boolean isSatisfied(final Object validatedObject, final Object valueToValidate, final OValContext context,
			final Validator validator) throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setErrorCode(final String errorCode) throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setMessage(final String message) throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setSeverity(final int severity) throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}
}
