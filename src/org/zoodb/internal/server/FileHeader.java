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
	private int[] rootPages = new int[2];
	private ArrayList<String> error = new ArrayList<>();
	
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
