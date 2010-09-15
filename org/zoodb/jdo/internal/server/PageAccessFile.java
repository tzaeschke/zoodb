package org.zoodb.jdo.internal.server;

import java.io.IOException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public interface PageAccessFile extends SerialInput, SerialOutput {

	void seekPage(int nextPage);

	void seekPage(int i, int j);

	void checkOverflow(int nextPage) throws IOException;

	@Deprecated
	int allocatePage();

	void writeString(String cName) throws IOException;

	String readString() throws IOException;	

	void close();

	void flush();

	int getOffset();

	void assurePos(int currentPage, int currentOffs);

	void lock();

	void unlock();

	int allocateAndSeek();

	int statsGetWriteCount();	
}
