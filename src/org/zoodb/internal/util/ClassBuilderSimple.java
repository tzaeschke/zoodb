package org.zoodb.internal.util;

import java.nio.ByteBuffer;

public class ClassBuilderSimple {

	private static final byte[] BA0_1 = {
		-54, -2, -70, -66,  // 0-3: magic number 
		0, 0,  //4-5: minor version 
		0, 50,  //6-7: major version 
		0, 16,  //8-9: constant pools size+1
		7, 0, 2, 								//CONST 1
	};
	
	private static final byte[] BA2 = {
		1, 0, 11, 98, 99, 101, 47,
		77, 121, 67, 108, 97, 115, 115,			//CONST 2 "bce/MyClass"
	};
	
	private static final byte[] BA3 = {
		7, 0, 4, 								//CONST 3  
	};
	
	private static final byte[] BA4 = {
		1, 0, 25, 98, 99, 101, 47, 80, 101, 114, 
		115, 105, 115, 116, 101, 110, 116, 67, 97, 112, 
		97, 98, 108, 101, 73, 109, 112, 108, 	//CONST 4	"bce/PersistentCapableImpl"
	};
	
	private static final byte[] BA5_12 = {
		1, 0, 6, 60, 105, 110, 105, 116, 62,  	//CONST 5 
		1, 0, 3, 40, 41, 86,  					//CONST 6	"()V" 
		1, 0, 4, 67, 111, 100, 101, 			//CONST 7	"Code"
		10, 0, 3, 0, 9,							//CONST 8 
		12, 0, 5, 0, 6, 						//CONST 9
		1, 0, 15, 76, 105, 110, 101, 78, 117, 109, 
		98, 101, 114, 84, 97, 98, 108, 101,		//CONST 10 
		1, 0, 18, 76, 111, 99, 97, 108, 86, 97, 114, 105, 
		97, 98, 108, 101, 84, 97, 98, 108, 101,	//CONST 11 
		1, 0, 4, 116, 104, 105, 115,			//CONST 12 "this" 
	};
	
	private static final byte[] BA13 = {
		1, 0, 13, 76, 98, 99, 101, 47, 77, 121, 67, 108, 97, 115, 
		115, 59,								//CONST 13 "Lbce/MyClass;" 
	};
	
	private static final byte[] BA14 = {
		1, 0, 10, 83, 111, 117, 114, 99, 
		101, 70, 105, 108, 101,					//CONST 14 
	};
	
	private static final byte[] BA15 = {
		1, 0, 12, 77, 121, 
		67, 108, 97, 115, 115, 46, 106, 97, 118, 97, //CONST 15	"MyClass.java"
	};
	
	private static final byte[] BA_end = {
		0, 33, 0, 1, 0, 3, 0, 0, 0, 0, 
		0, 1, 0, 1, 0, 5, 0, 6, 0, 1, 
		0, 7, 0, 0, 0, 47, 0, 1, 0, 1, 
		0, 0, 0, 5, 42, -73, 0, 8, -79, 0, 
		0, 0, 2, 0, 10, 0, 0, 0, 6, 0, 
		1, 0, 0, 0, 3, 0, 11, 0, 0, 0, 
		12, 0, 1, 0, 0, 0, 5, 0, 12, 0, 
		13, 0, 0, 0, 1, 0, 14, 0, 0, 0, 
		2, 0, 15
	};

	
//	private String className;
//	private String superName;
//	
//	
//	private ClassBuilderSimple(String className, String superName) {
//		this.className = className;
//		this.superName = superName;
//	}
	
	public static byte[] build(String className, String superName) {
		ByteBuffer bb = ByteBuffer.allocate(1000);
		bb.put(BA0_1);
		
		//bb.put(BA2);
		writeUtf8(bb, convertDots(className));
		
		bb.put(BA3);
		
		//bb.put(BA4);
		writeUtf8(bb, convertDots(superName));
		
		bb.put(BA5_12);
		
		//bb.put(BA13);
		writeUtf8(bb, "L" + convertDots(className) + ";");
		
		bb.put(BA14);
		
		//bb.put(BA15);
		String fName = convertDots(className);
		fName = fName.substring(fName.lastIndexOf('/') + 1);
		fName += ".java";
		writeUtf8(bb, fName);
		
		bb.put(BA_end);
		
		byte[] ba = new byte[bb.position()];
		bb.rewind();
		bb.get(ba);
		return ba;
	}
	
	private static void writeUtf8(ByteBuffer bb, String str) {
		bb.put((byte)ClassDebugger.CP.CONSTANT_Utf8.ordinal());
		bb.putShort((short) str.length());
		for (int i = 0; i < str.length(); i++) {
			bb.put((byte)str.charAt(i));
		}
	}
	
	private static final String convertDots(String name) {
		StringBuilder sb = new StringBuilder(name.length());
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i); 
			if (c == '.') {
				sb.append('/');
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}

}
