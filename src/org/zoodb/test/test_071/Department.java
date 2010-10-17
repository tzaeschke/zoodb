package org.zoodb.test.test_071;

import java.util.Collection;

public class Department {
	String name;
	Collection<Employee> emps;
	
	private Department() {
		// for JDO
	}

	
	public Department(String name) {
		this.name = name;
	}
}
