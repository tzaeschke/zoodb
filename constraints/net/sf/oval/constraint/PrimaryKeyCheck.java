package net.sf.oval.constraint;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.List;

import javax.jdo.ObjectState;
import javax.jdo.Query;

import ch.ethz.oserb.ConstraintManager;
import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AbstractAnnotationCheck;
import net.sf.oval.context.OValContext;
import net.sf.oval.exception.OValException;

public class PrimaryKeyCheck extends AbstractAnnotationCheck<PrimaryKey>{
	
	/**
	 * generated serial version uid.
	 */
	private static final long serialVersionUID = 2857169880015248488L;
	private String[] keys;
	
	@Override
	public void configure(final PrimaryKey primaryKeyAnnotation)
	{
		super.configure(primaryKeyAnnotation);
		this.keys = primaryKeyAnnotation.keys();
		
	}

		
	@Override
	public boolean isSatisfied(Object validatedObject, Object valueToValidate, OValContext context, Validator validator) throws OValException {
		// not null
		
		// unique
		
		
		// get instance of constraint manager to query db
		ConstraintManager cm;
		try {
			cm = ConstraintManager.getInstance();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		// check db for corresponding entry
		StringBuilder filter = attr+"=="+valueToValidate;
		Query query = cm.newQuery (validatedObject.getClass(), filter);
		@SuppressWarnings("unchecked")
		List<Object> results = (List<Object>) query.execute();
		if (results.size()!=0) return true;
		
		// check managed object for corresponding entry
		try {
			Field field = clazz.getField(attr);
			for(Object obj:cm.getManagedObjects(EnumSet.of(ObjectState.PERSISTENT_DIRTY, ObjectState.PERSISTENT_NEW),clazz)){
				if(field.get(obj)==valueToValidate)return true;
			}
		}catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		
		// if no corresponding found
		return false;
	}

}
