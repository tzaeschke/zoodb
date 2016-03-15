/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.util.ArrayList;

import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooSchemaImpl;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;

public class DBTracer {

	/** Create traces? Default is 'false' */
	public static boolean TRACE = false;
	private static final ArrayList<Call> calls = new ArrayList<>(); 
	
	private static final class Call {
		final String callee;
		final String caller;
		final String[] args;
		
		Call(String callee, String caller, String ... args) {
			this.callee = callee;
			this.caller = caller;
			this.args = args;
		}
		
		@Override
		public String toString() {
			String s = callee + "(";
			for (int i = 0; i < args.length-1; i++) {
				s += args[i] + ", ";
			}
			if (args.length > 0) {
				s += args[args.length-1];
			}
			s+= ");  // " + caller;
			return s;
		}
	}
			
	public static void reset() {
		calls.clear();
	}
	
	/**
	 * Enable or disable tracing.
	 */
	public static void enable(boolean enable) {
		TRACE = enable;
	}
	
	public static void logCall(Object calledObj, Object ... args) {
		if (!TRACE) {
			return;
		}
		
		StackTraceElement[] callStack = new RuntimeException().getStackTrace();
		StackTraceElement calledMethod = callStack[1];//callStack.length-2]; 
		StackTraceElement callingMethod = callStack[2];//callStack.length-3];
		String callee = calledMethod.getClassName() + "." + calledMethod.getMethodName();
		String caller = callingMethod.getClassName() + "." + callingMethod.getMethodName();
		if (caller.startsWith("org.zoodb.")) {
			return; //ignore
		}
		
		if (calledObj instanceof ZooPC) {
			callee = "/*" + calledObj.getClass().getSimpleName() + "*/";
			callee += "o" + ((ZooPC)calledObj).jdoZooGetOid() + "." + calledMethod.getMethodName();
		} else if (calledObj instanceof PersistenceManager) {
			callee = "pm" + "." + calledMethod.getMethodName();
			if (calledMethod.getMethodName().equals("currentTransaction")) {
				callee = "tx = " + callee;
			}
		} else if (calledObj instanceof Transaction) {
			callee = "tx" + "." + calledMethod.getMethodName();
		} else if (calledObj == ZooJdoHelper.class) {
			callee = ((Class<?>)calledObj).getSimpleName() + "." + calledMethod.getMethodName();
			if (calledMethod.getMethodName().equals("schema")) {
				callee = "ZooSchema zSchema = " + callee;
			}
		} else if (calledObj instanceof ZooSchemaImpl) {
			callee = "zSchema" + "." + calledMethod.getMethodName();
		} else if (calledObj == ZooJdoProperties.class) {
			callee = ((Class<?>)calledObj).getSimpleName() + "." + calledMethod.getMethodName();
			if (calledMethod.getMethodName().equals("<init>")) {
				callee = ZooJdoProperties.class.getSimpleName() + " props = new " + 
						ZooJdoProperties.class.getSimpleName();
			}
		}
		
		//We have to serialize it here, because otherwise the value may change...
		String[] argStr = new String[args.length];
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof ZooPC) {
				argStr[i] = "o"+((ZooPC)args[i]).jdoZooGetOid() + "/* " 
						+ args[i].getClass().getSimpleName() + " */";
			} else if (args[i] instanceof Class) {
				argStr[i] = ((Class<?>)args[i]).getSimpleName() + ".class";
			} else if (args[i] == null) {
				argStr[i] = "null";
			} else if (args[i] instanceof String) {
				argStr[i] = "\"" + args[i].toString() + "\"";
			} else if (args[i] instanceof PersistenceManager) {
				argStr[i] = "pm";
			} else {
				argStr[i] = args[i].toString();
			}
		}
		Call c = new Call(callee, caller, argStr);
		calls.add(c);
	}
	
	public static String print() {
		FormattedStringBuilder fm = new FormattedStringBuilder();
		for (Call c: calls) {
			fm.appendln(c.toString());
		}
		reset();
		return fm.toString();
	}
	
}
