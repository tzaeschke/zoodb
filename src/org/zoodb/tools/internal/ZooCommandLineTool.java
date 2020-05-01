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
package org.zoodb.tools.internal;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.zoodb.tools.ZooQuery;

public abstract class ZooCommandLineTool {

	protected static PrintStream out = System.out;
	protected static PrintStream err = System.err;
	
	private static final ByteArrayOutputStream BA = new ByteArrayOutputStream();

	
	public static void enableStringOutput() {
		ZooQuery.out = new PrintStream(BA);
	}
	
	public static String getStringOutput() {
		return BA.toString(); 
	}

	public static void resetStringOutput() {
		BA.reset();
	}
}
