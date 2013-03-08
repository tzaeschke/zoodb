package org.zoodb.profiler.test.test_071;

import java.util.Collection;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.test_071.Employee;

public class Department extends PersistenceCapableImpl {
	String name;
	Collection<Employee> emps;
	
	private Department() {
		// for JDO
	}

	
	public Department(String name) {
		this.name = name;
	}
}
