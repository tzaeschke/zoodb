/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.util;

import java.lang.reflect.Constructor;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	
	public static final Logger LOGGER = LoggerFactory.getLogger(DBLogger.class);
	
	public static final Class<? extends RuntimeException> USER_EXCEPTION;
	public static final Class<? extends RuntimeException> FATAL_USER_EXCEPTION;
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
			FATAL_USER_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalUserException");
			FATAL_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalException");
			FATAL_DATA_STORE_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalDataStoreException");
			FATAL_INTERNAL_EXCEPTION = ReflTools.classForName("javax.jdo.JDOFatalInternalException");
			OBJ_NOT_FOUND_EXCEPTION = ReflTools.classForName("javax.jdo.JDOObjectNotFoundException");
			OPTIMISTIC_VERIFICATION_EXCEPTION = ReflTools.classForName("javax.jdo.JDOOptimisticVerificationException");
			isJDO = true;
		} else {
			//Native ZooDB
			USER_EXCEPTION = ZooException.class;
			FATAL_USER_EXCEPTION = ZooException.class;
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
	 * @param level The maximum output level
	 * @deprecated (TZ 2017-05-26) Please use slf4j for logging configuration
	 */
	@Deprecated
	public static void setVerbosityLevel(int level) {
		LOGGER.error("Please use slf4j for logging configuration");
	}
	
	/**
	 * Set the level of the logger.
	 * @param level The output level
	 * @param redirectOutputToConsole Whether to redirect output to console
	 * @deprecated (TZ 2017-05-26) Please use slf4j for logging configuration
	 */
	@Deprecated
	public static void setLoggerLevel(Level level, boolean redirectOutputToConsole) {
		LOGGER.error("Please use slf4j for logging configuration");
	}

	private static RuntimeException newEx(Class<? extends RuntimeException> exCls, String msg, 
			Throwable cause) {
		return newEx(exCls, msg, cause, null);
	}
	
	
	private static RuntimeException newEx(Class<? extends RuntimeException> exCls, String msg, 
			Throwable cause, Object failed) {
		//severe(msg);
		Constructor<? extends RuntimeException> con;
		con = ReflTools.getConstructor(exCls, String.class, Throwable.class, Object.class);
		return ReflTools.newInstance(con, msg, cause, failed);
	}    
    
    public static RuntimeException newUser(String msg) {
    	return newEx(USER_EXCEPTION, msg, null);
    }    
    
    public static RuntimeException newUser(Logger logger, String msg, Object o1) {
    	logger.error(msg, o1);
    	return newEx(USER_EXCEPTION, msg, null);
    }    
    
    public static RuntimeException newUser(String msg, Object o1, Object o2) {
    	return newEx(USER_EXCEPTION, msg, null);
    }    
    
    public static RuntimeException newUser(String msg, Object o1, Object o2, Object o3) {
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

	public static RuntimeException newFatalUser(String msg) {
		return newFatalUser(msg, null);
    }    
    
	public static RuntimeException newFatalUser(String msg, Throwable t) {
    	return newEx(FATAL_USER_EXCEPTION, msg, t);
	}

	public static RuntimeException newObjectNotFoundException(String msg) {
		return newObjectNotFoundException(msg, null, null);
	}

	public static RuntimeException newObjectNotFoundException(String msg, Throwable t, 
			Object failed) {
    	return newEx(OBJ_NOT_FOUND_EXCEPTION, msg, t, failed);
	}

	public static RuntimeException newFatalInternal(String msg) {
		return newFatalInternal(msg, null);
	}

	public static RuntimeException newFatalInternal(String msg, Throwable t) {
    	return newEx(FATAL_INTERNAL_EXCEPTION, msg, t);
	}

	/**
	 * These always result in the session being closed!
	 * @param msg The error message
	 * @param t The Throwable to report
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

	public static RuntimeException wrap(Exception e) {
		return new RuntimeException(e);
	}
}
