package org.zoodb.test.data;

public interface Visitor<T> {

	void visit(T holder);

}
