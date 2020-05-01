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
package org.zoodb.test.jdo.classes;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class TC0 extends PersistenceCapableImpl {
	
	private int i0;

	public TC0() {
		//empty
	}
	
	public TC0(int i0) {
		this.i0 = i0;
	}

	public int getI0() {
		return i0;
	}

	public void setI0(int i0) {
		this.i0 = i0;
	}

	
	
}
