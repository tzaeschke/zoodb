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
