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
package org.zoodb.internal.server;

import java.util.ArrayList;
import java.util.List;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.tools.ZooConfig;

public class FileHeader {

	private int fileID;
	private int versionMinor;
	private int versionMajor;
	private int pageSize;
	private final int[] rootPages = new int[2];
	private final ArrayList<String> error = new ArrayList<>();
	
	public static FileHeader read(StorageChannelInput in) {
		FileHeader header = new FileHeader();
		
		in.seekPageForRead(PAGE_TYPE.DB_HEADER, 0);
		int fid = in.readInt();
		if (fid != DiskIO.DB_FILE_TYPE_ID) { 
			header.error.add("This is not a ZooDB file (illegal file ID: " + fid + ")");
		}
		int maj = in.readInt();
		int min = in.readInt();
		if (maj != DiskIO.DB_FILE_VERSION_MAJ) { 
		    header.error.add("Illegal major file version: " + maj + "." + min +
		            "; Software version: " + 
		            DiskIO.DB_FILE_VERSION_MAJ + "." + DiskIO.DB_FILE_VERSION_MIN);
		}
		if (min != DiskIO.DB_FILE_VERSION_MIN) { 
			header.error.add("Illegal minor file version: " + maj + "." + min +
					"; Software version: " + 
					DiskIO.DB_FILE_VERSION_MAJ + "." + DiskIO.DB_FILE_VERSION_MIN);
		}

		int pageSize = in.readInt();
		if (pageSize != ZooConfig.getFilePageSize()) {
			//TODO actually, in this case would should just close the file and reopen it with the
			//correct page size.
			header.error.add("Incompatible page size: " + pageSize);
		}
		
		//main directory
		header.rootPages[0] = in.readInt();
		header.rootPages[1] = in.readInt();
		
		header.fileID = fid;
		header.versionMinor = min;
		header.versionMajor = maj;
		header.pageSize = pageSize;
		return header;
	}

	public int getFileID() {
		return fileID;
	}

	public int getVersionMinor() {
		return versionMinor;
	}

	public int getVersionMajor() {
		return versionMajor;
	}

	public int getPageSize() {
		return pageSize;
	}

	public int[] getRootPages() {
		return rootPages;
	}

	public boolean successfulRead() {
	    return error.isEmpty();
	}
	
	public List<String> errorMsg() {
	    return error;
	}
}
