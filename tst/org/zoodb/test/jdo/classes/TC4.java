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

public class TC4 extends TC3 {
	
	private int i4;

	public TC4() {
		//empty
	}
	
	public TC4(int i0, int i1, int i2, int i3, int i4) {
		super(i0, i1, i2, i3);
		this.i4 = i4;
	}

	public int getI4() {
		return i4;
	}

	public void setI4(int i4) {
		this.i4 = i4;
	}
	
}
