package org.zoodb.jdo.internal.server;

import java.io.IOException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public interface PageAccessFile extends SerialInput, SerialOutput {

	void seekPage(int nextPage, boolean autoPaging);

	void seekPage(int i, int j, boolean autoPaging);

	void checkOverflow(int nextPage) throws IOException;

	@Deprecated
	int allocatePage(boolean autoPaging);

	void close();

	void flush();

	int getOffset();

    int getPage();  

	void assurePos(int currentPage, int currentOffs);

	void lock();

	void unlock();

	int allocateAndSeek(boolean autoPaging);

	int statsGetWriteCount();
}
