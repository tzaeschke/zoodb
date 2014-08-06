package org.zoodb.profiling.api;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;

public class Utils {
	
	//get the index of 'fieldUnderTest' in ZooClassDef.allFields
	public static int getIndexForFieldName(String fieldName, ZooClassDef def) {
		ZooFieldDef[] fds = def.getAllFields();
		
		for (int i=0;i<fds.length;i++) {
			if (fds[i].getName().equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}
	
	public static String getFieldNameForIndex(int index, ZooClassDef def) {
		ZooFieldDef zfd = def.getAllFields()[index];
		return zfd.getName();
	}

}
