/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal.util;

import java.lang.reflect.Constructor;
import java.util.logging.Logger;

import org.zoodb.api.ZooException;
import org.zoodb.api.impl.ZooPC;

public class DBLogger {

	private static final boolean isJDO;
	//TODO ENUM
//	enum EX_TYPE {
//		FATAL,
//		ILLEGAL_ARGUMENT,
//		ILLEGAL_STATE,
//		//OBJECT_NOT_FOUND, //?
//		USER; //repeatable
//	}
	
	private static final Logger _LOGGER = 
		Logger.getLogger(DBLogger.class.getName());

    private static int verbosityLevel = 0;
    private static boolean verboseToLog = false;
	
	public static final Class<? extends RuntimeException> USER_EXCEPTION;
	public static final Class<? extends RuntimeException> FATAL_EXCEPTION;
	//these always result in the session being closed!
	public static final Class<? extends RuntimeException> FATAL_DATA_STORE_EXCEPTION;
	public static final Class<? extends RuntimeException> FATAL_INTERNAL_EXCEPTION;
	public static final Class<? extends RuntimeException> OBJ_NOT_FOUND_EXCEPTION;
	public static final Class<? extends RuntimeException> OPTIMISTIC_VERIFICATION_EXCEPTION;

	static {
		if (ReflTools.existsClass("javax.jdo.JDOHelper")) {
			//try JDO
			USER_EXCEPTION = ReflTools.classForName("javax.jdo.JDOUserException");
			FATAL_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalException");
			FATAL_DATA_STORE_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalDataStoreException");
			FATAL_INTERNAL_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalInternalException");
			OBJ_NOT_FOUND_EXCEPTION = ReflTools.classForName("javax.jdo.JDOObjectNotFoundException");
			OPTIMISTIC_VERIFICATION_EXCEPTION = ReflTools.classForName("javax.jdo.JDOOptimisticVerificationException");
			isJDO = true;
		} else {
			//Native ZooDB
			USER_EXCEPTION = ZooException.class;
			FATAL_EXCEPTION = ZooException.class;
			FATAL_DATA_STORE_EXCEPTION = ZooException.class;
			FATAL_INTERNAL_EXCEPTION = ZooException.class;
			OBJ_NOT_FOUND_EXCEPTION = ZooException.class;
			OPTIMISTIC_VERIFICATION_EXCEPTION = ZooException.class;
			isJDO = false;
		}
	}
	
	/**
	 * Set the verbosity level for debug output. Level 0 means no output, higher levels result
	 * in increasingly detailed output. Default is 0.
	 * @param level
	 */
	public static void setVerbosityLevel(int level) {
		verbosityLevel = level;
	}
	
	private static RuntimeException newEx(Class<? extends RuntimeException> exCls, String msg, 
			Throwable cause) {
		return newEx(exCls, msg, cause, null);
	}
	
	
	private static RuntimeException newEx(Class<? extends RuntimeException> exCls, String msg, 
			Throwable cause, Object failed) {
		severe(msg);
		Constructor<? extends RuntimeException> con;
		con = ReflTools.getConstructor(exCls, String.class, Throwable.class, Object.class);
		return ReflTools.newInstance(con, msg, cause, failed);
	}
    
    
	/**
	 * Prints a debug message if the level is below or equal the 
	 * <code>verbosity</code> setting. The output can be
	 * redirected to the logging mechanism by setting the following
	 * property: <tt>verboseOutput = log</tt>.
	 * @param level
	 * @param message Message to print.
	 */
	public static final void debugPrint(int level, String ... message) {
		if (level <= verbosityLevel) {
			FormattedStringBuilder buf = 
				new FormattedStringBuilder().append("Debug: ").append(message);
			if (verboseToLog) {
				_LOGGER.info(buf.toString());
			} else {
				System.out.print(buf.toString());
			}
		}
	}

	/**
	 * Prints a debug message if the level is below or equal the 
	 * <code>verbosity</code> setting. The output can be
	 * redirected to the logging mechanism by setting the following
	 * property: <tt>verboseOutput = log</tt>.
	 * @param level
	 * @param message Message to print.
	 */
	public static final void debugPrintln(int level, String ... message) {
		debugPrint(level, message);
		if (level <= verbosityLevel && !verboseToLog) {
			System.out.println();
		}
	}

    public static void severe(String string) {
        System.err.println("SEVERE: " + string);
    }

    public static void warning(String string) {
        System.err.println("WARNING: " + string);
    }

    public static void info(String string) {
        System.out.println("INFO: " + string);
    }
    
    public static RuntimeException newUser(String msg) {
    	return newEx(USER_EXCEPTION, msg, null);
    }    
    
    public static RuntimeException newUser(String msg, Throwable t) {
    	return newEx(USER_EXCEPTION, msg, t);
    }

	public static RuntimeException newUser(String msg, ZooPC obj) {
    	if (isJDO) {
    		//throw new JDOUserException(msg, obj);
    		return newEx(USER_EXCEPTION, msg, null, obj);
    	}
    	return newEx(USER_EXCEPTION, msg + " obj=" + Util.getOidAsString(obj), null);
	}

	public static RuntimeException newFatal(String msg) {
		return newFatal(msg, null);
    }    
    
	public static RuntimeException newFatal(String msg, Throwable t) {
    	return newEx(FATAL_EXCEPTION, msg, t);
	}

	public static RuntimeException newObjectNotFoundException(String msg) {
		return newObjectNotFoundException(msg, null);
	}

	public static RuntimeException newObjectNotFoundException(String msg, Throwable t) {
    	return newEx(OBJ_NOT_FOUND_EXCEPTION, msg, t);
	}

	public static RuntimeException newFatalInternal(String msg) {
		return newFatalInternal(msg, null);
	}

	public static RuntimeException newFatalInternal(String msg, Throwable t) {
    	return newEx(FATAL_INTERNAL_EXCEPTION, msg, t);
	}

	/**
	 * THese always result in the session being closed!
	 * @param msg
	 * @param t
	 * @return Fatal data store exception.
	 */
	public static RuntimeException newFatalDataStore(String msg, Throwable t) {
    	return newEx(FATAL_DATA_STORE_EXCEPTION, msg, t);
	}


	public static boolean isUser(RuntimeException e) {
		return USER_EXCEPTION.isAssignableFrom(e.getClass());
	}


	public static boolean isObjectNotFoundException(RuntimeException e) {
		return OBJ_NOT_FOUND_EXCEPTION.isAssignableFrom(e.getClass());
	}


	public static boolean isFatalDataStoreException(RuntimeException e) {
		return FATAL_DATA_STORE_EXCEPTION.isAssignableFrom(e.getClass());
	}


	public static boolean isOptimisticVerificationException(RuntimeException e) {
		return OPTIMISTIC_VERIFICATION_EXCEPTION.isAssignableFrom(e.getClass());
	}
}
