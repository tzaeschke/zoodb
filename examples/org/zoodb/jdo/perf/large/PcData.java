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
package org.zoodb.jdo.perf.large;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

@SuppressWarnings("unused")
public class PcData extends PersistenceCapableImpl {

	private float f1;
	private float f2;
	private String s1;
	private String s2;
	private String s3;
	private String s4;
	
	private PcData() {
		//for JDO
	}
	
	public PcData(String s1, String s2, String s3, String s4, float f1, float f2) {
		this.s1 = s1;
		this.s2 = s2;
		this.s3 = s3;
		this.s4 = s4;
		this.f1 = f1;
		this.f2 = f2;
	}

	public String getS1() {
		zooActivateRead();
		return s1;
	}
	
}
