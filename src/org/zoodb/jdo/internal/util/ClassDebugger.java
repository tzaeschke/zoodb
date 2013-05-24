package org.zoodb.jdo.internal.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ClassDebugger {

	private static final byte[] BA = {
		-54, -2, -70, -66,  // 0-3: magic number 
		0, 0,  //4-5: minor version 
		0, 50,  //6-7: major version 
		0, 16,  //8-9: constant pools size+1
		7, 0, 2, 1, 0, 11, 98, 99, 101, 47, 
		77, 121, 67, 108, 97, 115, 115, 7, 0, 4, 
		1, 0, 25, 98, 99, 101, 47, 80, 101, 114, 
		115, 105, 115, 116, 101, 110, 116, 67, 97, 112, 
		97, 98, 108, 101, 73, 109, 112, 108, 1, 0, 
		6, 60, 105, 110, 105, 116, 62, 1, 0, 3, 
		40, 41, 86, 1, 0, 4, 67, 111, 100, 101, 
		10, 0, 3, 0, 9, 12, 0, 5, 0, 6, 
		1, 0, 15, 76, 105, 110, 101, 78, 117, 109, 
		98, 101, 114, 84, 97, 98, 108, 101, 1, 0, 
		18, 76, 111, 99, 97, 108, 86, 97, 114, 105, 
		97, 98, 108, 101, 84, 97, 98, 108, 101, 1, 
		0, 4, 116, 104, 105, 115, 1, 0, 13, 76, 
		98, 99, 101, 47, 77, 121, 67, 108, 97, 115, 
		115, 59, 1, 0, 10, 83, 111, 117, 114, 99, 
		101, 70, 105, 108, 101, 1, 0, 12, 77, 121, 
		67, 108, 97, 115, 115, 46, 106, 97, 118, 97, 
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

	
	public enum CP {
		CONSTANT_0,
		CONSTANT_Utf8, // 1 Länge des unterminierten Strings in Anzahl Byte (2 bytes)
		//String in Utf8-Darstellung (variable Länge)
		CONSTANT_Unicode, // 2 Länge nachfolgenden Zeichenkette in Anzahl Byte (2 bytes)
		//Unicode-Zeichenkette (variable Länge)
		CONSTANT_Integer, // 3 Vorzeichenbehafteter Integer-Wert, big-endian (4 bytes)
		CONSTANT_Float, // 4 Float-Wert nach IEEE 754 (4 bytes)
		CONSTANT_Long, // 5 Vorzeichenbehafteter Integer-Wert, big-endian (8 bytes)
		CONSTANT_Double, // 6 Double-Wert nach IEEE 754 (8 bytes)
		CONSTANT_Class, // 7 Index zu einem weiteren Konstantpool-Eintrag vom Typ CONSTANT_Utf8, der
		//den Klassennamen beinhaltet (2 bytes)
		CONSTANT_String, // 8 Index zu einem weiteren Konstantpool-Eintrag vom Typ CONSTANT_Utf8, der
		//den String beinhaltet (2 bytes)
		CONSTANT_Fieldref, // 9 Index zu einem CONSTANT_Class Eintrag, der die Klasse bezeichnet, die das
		//Feld enthält (2 bytes) plus ein Index zu einem CONSTANT_NameAndType Eintrag,
		//der die Signatur des Feldes angibt (2 bytes)
		CONSTANT_Methodref, // A Index zu einem CONSTANT_Class Eintrag, der die Klasse bezeichnet, welche die
		//Methode enthält (2 bytes) plus ein Index zu einem CONSTANT_NameAndType
		//Eintrag, der die Signatur der Methode angibt (2 bytes)
		CONSTANT_InterfaceMethodref, // B Index zu einem CONSTANT_Class Eintrag, der das Interface bezeichnet, welches
		//die jeweilige Methode deklariert (2 bytes) plus ein Index zu einem
		//CONSTANT_NameAndType Eintrag, der die Signatur des Interfaces angibt (2
		//bytes)
		CONSTANT_NameAndType, // C Index zu einem CONSTANT_Utf8 Eintrag, der den Namen des Feldes oder der
		//Methode enthält (2 bytes) plus ein weiteren Index zu einem CONSTANT_Utf8
		//Eintrag, der die Signatur des Feldes oder der Methode angibt (2 bytes)
	}
	
	
	private static final int ACC_PUBLIC = 0x0001;// Kann ausserhalb des Package verwendet werden.
	private static final int ACC_PRIVATE = 0x0002; // Kann nur von innerhalb der Klasse zugegriffen werden.
	private static final int ACC_PROTECTED = 0x0004; // Kann von innerhalb der Klasse und von abgeleiteten Klassen
										// zugegriffen werden.
	private static final int ACC_STATIC = 0x0008; // Als static deklariert (Klassenvariable).	
	private static final int ACC_FINAL = 0x0010; // Kann nicht abgeleitet werden.
	private static final int ACC_SUPER = 0x0020; // Behandelt Methodenaufrufe der Superklasse speziell. Dieses
												 // Flag dient zur Rückwärtskompatibilität und wird von neueren
												 // Compilern immer gesetzt.
	private static final int ACC_VOLATILE = 0x0040; // Kann nicht ge-cached werden.
	private static final int ACC_TRANSIENT = 0x0080; // Feld als transient deklariert.
	private static final int ACC_NATIVE = 0x0100; // In einer anderen Sprache (als Java) implementiert.
	private static final int ACC_INTERFACE = 0x0200; // Dieses Flag ist gesetzt, wenn es sich um ein Interface und
													// nicht um eine Klasse handelt.
	private static final int ACC_ABSTRACT = 0x0400; // Kann nicht instanziert werden.
	private static final int ACC_STRICT = 0x0800; // Als strictfp deklariert.
	
	
	private static FormattedStringBuilder sb = new FormattedStringBuilder();
	
	public static boolean VERBOSE = true;
	
	public static void main(String[] args) {
		run(BA);
		byte[] ba2 = ClassBuilderSimple.build("MySubClass", "MySuperClass");
		run(ba2);
	}

	private static void run(byte[] ba) {
		ByteBuffer bb = ByteBuffer.wrap(ba);
		
		logHex("Magic:", bb.get(), bb.get(), bb.get(), bb.get());
		log("Version:", bb.getShort(), bb.getShort());
		int poolSize = bb.getShort();
		log("Pool size:", poolSize);
		for (int i = 1; i < poolSize; i++) {
			int tag = bb.get();
			CP cp = CP.values()[tag];
			log("entry=" + i + ":", Integer.toString(tag), cp.name());
			switch(cp) {
			case CONSTANT_Utf8: {
				int len = bb.getShort();
				sb.fill(20);
				for (int c = 0; c < len; c++) {
					sb.append((char)bb.get());
				}
				sb.appendln();
				break;
			}
			case CONSTANT_Class: {
				int id = bb.getShort();
				log("     ID=", id);
				break;
			}
			case CONSTANT_Methodref: {
				int idClass = bb.getShort();
				int idType = bb.getShort();
				log("     classID=", idClass + " typeNameID=" + idType);
				break;
			}
			case CONSTANT_NameAndType: {
				int idName = bb.getShort();
				int idType = bb.getShort();
				log("     nameID=", idName + " typeID=" + idType);
				break;
			}
			default: throw new UnsupportedOperationException(cp.name());
			}
		}
		sb.appendln("p=" + bb.position());
		
		sb.appendln();
		sb.appendln("Class descriptor");
		sb.appendln("================");
		int classMods = bb.getShort();
		sb.append("Modifiers:").fill(20);
		getModifiers(classMods, true);
		sb.appendln();
		
		sb.append("Name:").fill(20);
		int thisID = bb.getShort();
		sb.appendln("ID=" + thisID);
		int superID = bb.getShort();
		sb.append("Super-class").fill(20);
		sb.appendln("ID=" + superID);
		int nInter = bb.getShort();
		sb.append("Interfaces").fill(20);
		sb.appendln("n=" + nInter);
		
		sb.appendln();
		sb.appendln("Fields");
		sb.appendln("================");
		int nFields = bb.getShort();
		sb.append("Fields").fill(20);
		sb.appendln("n=" + nFields);
		
		sb.appendln();
		sb.appendln("Methods");
		sb.appendln("================");
		int nMethods = bb.getShort();
		sb.append("Methods").fill(20);
		sb.appendln("n=" + nMethods);
		for (int c = 0; c < nMethods; c++) {
			int mods = bb.getShort();
			sb.append("Method").fill(20);
			getModifiers(mods, false);
			int nameID = bb.getShort();
			sb.append("nameID=" + nameID);
			int signID = bb.getShort();
			sb.append("  signatureID=" + signID);
			sb.appendln();
			sb.fill(20);
			int nAttr = bb.getShort();
			sb.append("attributes=" + nAttr);
			sb.appendln();

			for (int a = 0; a < nAttr; a++) {
				int attrID = bb.getShort();
				int attrLen = bb.getInt();
				byte[] mba = new byte[attrLen]; 
				bb.get(mba);
				sb.fill(20); 
				sb.append("ID=" + attrID);
				sb.append("  len=" + attrLen);
				sb.append("  code=" + Arrays.toString(mba));
				sb.appendln();
			}
		}
		
		sb.appendln();
		sb.appendln("Class attributes");
		sb.appendln("================");
		int nClassAttr = bb.getShort();
		sb.append("Attributes").fill(20);
		sb.appendln("n=" + nClassAttr);
		for (int a = 0; a < nClassAttr; a++) {
			int attrID = bb.getShort();
			int attrLen = bb.getInt();
			byte[] mba = new byte[attrLen]; 
			bb.get(mba);
			sb.fill(20); 
			sb.append("ID=" + attrID);
			sb.append("  len=" + attrLen);
			sb.append("  code=" + Arrays.toString(mba));
			sb.appendln();
		}

		
//		int classMods = bb.getShort();
//		sb.append("Modifiers:").fill(20);
//		if ((classMods & ACC_PUBLIC) != 0) sb.append("public ");
//		if ((classMods & ACC_FINAL) != 0) sb.append("final ");
//		if ((classMods & ACC_SUPER) != 0) sb.append("super ");
//		if ((classMods & ACC_ABSTRACT) != 0) sb.append("abstract ");
//		if ((classMods & ACC_INTERFACE) != 0) sb.append("interface ");
//		else sb.append("class ");
//		sb.appendln();
//		sb.append("Name:").fill(20);
//		int thisID = bb.getShort();
//		sb.appendln("ID=" + thisID);
//		int superID = bb.getShort();
//		sb.append("Super-class").fill(20);
//		sb.appendln("ID=" + superID);
//		int nInter = bb.getShort();
//		sb.append("Interfaces").fill(20);
//		sb.appendln("n=" + nInter);
		
		sb.appendln("p=" + bb.position() + "/" + ba.length);
		if (VERBOSE) {
			System.out.println(sb);
		}
	}


	private static void getModifiers(int mods, boolean isClass) {
		if ((mods & ACC_PUBLIC) != 0) sb.append("public ");
		if ((mods & ACC_PRIVATE) != 0) sb.append("private ");
		if ((mods & ACC_PROTECTED) != 0) sb.append("protected ");
		if ((mods & ACC_STATIC) != 0) sb.append("static ");
		if ((mods & ACC_FINAL) != 0) sb.append("final ");
		if (isClass) {
			if ((mods & ACC_SUPER) != 0) sb.append("super ");
		} else {
			if ((mods & ACC_SUPER) != 0) sb.append("synchronized ");
		}
		if ((mods & ACC_ABSTRACT) != 0) sb.append("abstract ");
		if (isClass) {
			if ((mods & ACC_INTERFACE) != 0) sb.append("interface ");
			else sb.append("class ");
		}
	}
	
	private static void logHex(String string, byte ... bytes) {
		sb.append(string).fill(20);
		for (byte b: bytes) {
			sb.append(toHex(b));
			sb.append(" ");
		}
		sb.appendln();
	}
	
	private static void log(String string, int ... values) {
		sb.append(string).fill(20);
		for (int v: values) {
			sb.append(v);
			sb.append(" ");
		}
		sb.appendln();
	}
	
	private static void log(String string, String ... values) {
		sb.append(string).fill(20);
		for (String v: values) {
			sb.append(v);
			sb.append(" ");
		}
		sb.appendln();
	}
	
	private static String toHex(byte b) {
		String s = Integer.toHexString(b);
		return s.substring(6);
	}
	
}
