package org.zoodb.tools;

public class ZooConfig {
	
	public static final int MODEL_1P = 1; 
	public static final int MODEL_2P = 2;
	public static int MODEL = MODEL_1P;

	private static final String preZoo = "org.zoodb.";
	private static final String preServer = "org.zoodb.internal.server.";
	
//	public static final String FILE_PAF_BB = preServer + "StorageFile_BBRoot";
//	//public static final String FILE_PAF_BB = preServer + "StorageInMemory";
//	public static final String FILE_PAF_IN_MEMORY = preServer + "StorageInMemory";
	public static final String FILE_PAF_BB = preServer + "StorageRootFile";
	//public static final String FILE_PAF_BB = preServer + "StorageInMemory";
	public static final String FILE_PAF_IN_MEMORY = preServer + "StorageRootInMemory";

	public static final String FILE_MGR_IN_MEMORY = preZoo + "tools.impl.DataStoreManagerInMemory";
	public static final String FILE_MGR_ONE_FILE = preZoo + "tools.impl.DataStoreManagerOneFile";
	//public static final String FILE_MGR_ONE_FILE = FILE_MGR_IN_MEMORY; 

	public static final int FILE_PAGE_SIZE_DEFAULT = 1024*4;  //bytes

	
	private static String fileDefault = FILE_PAF_BB;
	private static String fileManagerDefault = FILE_MGR_ONE_FILE;
	private static int defaultPageSize = FILE_PAGE_SIZE_DEFAULT;

	public static void setDefaults() {
		fileDefault = FILE_PAF_BB;
		fileManagerDefault = FILE_MGR_ONE_FILE;
		defaultPageSize = FILE_PAGE_SIZE_DEFAULT;
	}
	
	public static void setFileManager(String className) {
		fileManagerDefault = className;
		if (className.equals(FILE_MGR_ONE_FILE)) {
			fileDefault = FILE_PAF_BB;
		} else if (className.equals(FILE_MGR_IN_MEMORY)) {
			fileDefault = FILE_PAF_IN_MEMORY;
		}
	}

	public static String getFileProcessor() {
		return fileDefault;
	}

	public static String getFileManager() {
		return fileManagerDefault;
	}

	public static int getFilePageSize() {
		return defaultPageSize;
	}

	/**
	 * 
	 * @param pageSize page size in bytes.
	 */
	public static void setFilePageSize(int pageSize) {
		defaultPageSize = pageSize;
	}
}
