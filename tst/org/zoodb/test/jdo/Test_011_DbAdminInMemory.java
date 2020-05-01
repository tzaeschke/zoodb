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
package org.zoodb.test.jdo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.zoodb.tools.ZooConfig;

public class Test_011_DbAdminInMemory extends Test_010_DbAdmin {

	@BeforeClass
	public static void setUpClass() {
		ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
	}
	
	//Test are in super-class
	
	@AfterClass
	public static void tearDownClass() {
		ZooConfig.setDefaults();
	}
}
