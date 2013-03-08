package org.zoodb.profiler.test.test_071;

import org.zoodb.profiler.test.test_071.Employee;

public class Info {
	public String name;
	public Float salary;
	public Employee reportsTo;
	public Info (String name, Float salary, Employee reportsTo) {
		this.name = name;
		this.salary = salary;
		this.reportsTo = reportsTo;
	}
}
