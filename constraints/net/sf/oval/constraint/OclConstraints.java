/*******************************************************************************
 * 
 *******************************************************************************/
package net.sf.oval.constraint;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.sf.oval.configuration.annotation.Constraint;

/**
 * Check if the object satisfies the all specified ocl constraints.
 * 
 * @author oserb
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Constraint(checkWith = OclConstraintsCheck.class)
public @interface OclConstraints
{
	OclConstraint[] value();
}
