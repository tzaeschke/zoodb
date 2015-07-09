/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;

public class Test_039_SchemaEvolBug {

	private static ZooJdoProperties props;

	@BeforeClass
	public static void beforeClass() {
		props = new ZooJdoProperties(TestTools.getDbName());
		props.setZooAutoCreateSchema(true);
	}

	@Before
	public void before() {
		TestTools.closePM();
		TestTools.removeDb();
		TestTools.createDb();
	}

	@Test
	public void testIssue66() throws URISyntaxException {
		//populate
		TestTools.createDb();
		TestTools.defineSchema(FilePCv1.class, UserPC.class, AssetPCv1.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		//FilePCv1 f1 = new FilePCv1(123, "sPath", "mType", "oFileName");
		UserPC u1 = new UserPC(345, "userName");
		URI uri = new URI("http://www.zoodb.org");
		Date date = new Date(12345678);
		AssetPCv1 a1 = new AssetPCv1(234, uri, u1, date, 55);
		
		pm.makePersistent(a1);
		
		Object oidA1 = pm.getObjectId(a1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//evolve
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema zs = ZooJdoHelper.schema(pm);
		ZooClass sFile = zs.getClass(FilePCv1.class.getName());
		sFile.rename(FilePCv2.class.getName());

		ZooClass sAsset = zs.getClass(AssetPCv1.class.getName());
		sAsset.addField("file", sFile, 0);
		sAsset.addField("sourceFile", sFile, 0);

		ZooField zf1 = sAsset.getField("s1"); 
		ZooField zf2 = sAsset.getField("s2"); 
		ZooField zf3 = sAsset.getField("s3"); 
		
		//evolve 
		Iterator<ZooHandle> assetHandleIterator = sAsset.getHandleIterator(false);
		while (assetHandleIterator.hasNext()) {
			final ZooHandle assetHandle = assetHandleIterator.next();
//			final FilePCv2 file = new FilePCv2(
//					id,
//					(String) assetHandle.getValue("s1"),
//					(String) assetHandle.getValue("s2"),
//					(String) assetHandle.getValue("s3"));
			ZooHandle file = sFile.newInstance();
			file.setValue("storagePath", assetHandle.getValue("s1"));
			file.setValue("mimeType", assetHandle.getValue("s2"));
			file.setValue("originalFileName", assetHandle.getValue("s3"));
			
			assetHandle.setValue("file", file);
			assetHandle.setValue("sourceFile", null);
		}
		
		//delete fields
		zf1.remove();
		zf2.remove();
		zf3.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		sFile = zs.getClass(AssetPCv1.class.getName());
		sFile.rename(AssetPCv2.class.getName());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check evolved data
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		AssetPCv2 a2 = (AssetPCv2) pm.getObjectById(oidA1);
		assertEquals("myS1", a2.getFile().storagePath());
		assertEquals("myS2", a2.getFile().mimeType());
		assertEquals("myS3", a2.getFile().originalFileName());
		//TODO
		assertEquals(uri, a2.uri());
		assertEquals(date, a2.date());
		assertEquals(55, a2.fileArray().length);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testIssue66NoReflection() throws URISyntaxException {
		//populate
		TestTools.createDb();
		TestTools.defineSchema(FilePCv1.class, UserPC.class, AssetPCv1.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		//FilePCv1 f1 = new FilePCv1(123, "sPath", "mType", "oFileName");
		UserPC u1 = new UserPC(345, "userName");
		URI uri = new URI("http://www.zoodb.org");
		Date date = new Date(12345678);
		AssetPCv1 a1 = new AssetPCv1(234, uri, u1, date, 55);
		
		pm.makePersistent(a1);
		
		Object oidA1 = pm.getObjectId(a1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//evolve
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema zs1 = ZooJdoHelper.schema(pm);
		ZooClass sFile1 = zs1.getClass(FilePCv1.class.getName());
		sFile1.rename(FilePCv2.class.getName());

		//TODO remove
//		pm.currentTransaction().commit();
//		TestTools.closePM();
//		pm = TestTools.openPM();
//		pm.currentTransaction().begin();
		
		ZooSchema zs = ZooJdoHelper.schema(pm);
		ZooClass sFile = zs.getClass(FilePCv2.class.getName());
		
		ZooClass sAsset = zs.getClass(AssetPCv1.class.getName());
		sAsset.addField("file", sFile, 0);
		sAsset.addField("sourceFile", sFile, 0);

		ZooField zf1 = sAsset.getField("s1"); 
		ZooField zf2 = sAsset.getField("s2"); 
		ZooField zf3 = sAsset.getField("s3"); 
		
		//evolve 
		Iterator<ZooHandle> assetHandleIterator = sAsset.getHandleIterator(false);
		while (assetHandleIterator.hasNext()) {
			final ZooHandle assetHandle = assetHandleIterator.next();
			final FilePCv2 file = new FilePCv2(
					123,
					(String) assetHandle.getValue("s1"),
					(String) assetHandle.getValue("s2"),
					(String) assetHandle.getValue("s3"));
//			ZooHandle file = sFile.newInstance();
//			file.setValue("storagePath", assetHandle.getValue("s1"));
//			file.setValue("mimeType", assetHandle.getValue("s2"));
//			file.setValue("originalFileName", assetHandle.getValue("s3"));
//			
//			//TODO fix
			//the following can be used to prevent an NPE for empty context....
//			pm.makePersistent(file);
			
			//TODO use field handle
			assetHandle.setValue("file", file);
			assetHandle.setValue("sourceFile", null);
		}
		
		//delete fields
		zf1.remove();
		zf2.remove();
		zf3.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		sFile = zs.getClass(AssetPCv1.class.getName());
		sFile.rename(AssetPCv2.class.getName());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check evolved data
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		AssetPCv2 a2 = (AssetPCv2) pm.getObjectById(oidA1);
		assertEquals("myS1", a2.getFile().storagePath());
		assertEquals("myS2", a2.getFile().mimeType());
		assertEquals("myS3", a2.getFile().originalFileName());
		//TODO
		//assertEquals(uri, a2.uri());
		assertEquals(date, a2.date());
		assertEquals(55, a2.fileArray().length);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}


abstract class AbstractPC extends ZooPC {

	protected AbstractPC() {
		// default constructor for ZooDB
	}
}

abstract class AbstractIdentifiedPC extends AbstractPC {

	private long id;


	protected AbstractIdentifiedPC() {
		// default constructor for ZooDB
	}


	protected AbstractIdentifiedPC(final long id) {
		this.id = id;
	}


	public long getId() {
		this.zooActivateRead();
		return this.id;
	}

}


class AssetPCv2 extends AbstractIdentifiedPC {

	private URI origin;
	private Date importDate;
	private FilePCv2 file;
	private FilePCv2 sourceFile;
	private FilePCv2[] thumbnails;
	private FilePCv2 selectedThumbnail;
	private boolean onlyFirstThumbGenerated;
	private UserPC creatingUser;
	private UserPC thumbnailSettingUser;


	private AssetPCv2(){
		// hidden default constructor for ZooDB.
	}


	public FilePCv2[] fileArray() {
		return thumbnails;
	}


	public Date date() {
		return importDate;
	}


	public URI uri() {
		return origin;
	}


	public FilePCv2 getFile() {
		return file;
	}


	public AssetPCv2(final long id, final URI request, final UserPC creatingUser, Date date, int i) {
		super(id);
		this.origin = request;
		this.creatingUser = creatingUser;
		this.onlyFirstThumbGenerated = true;
		this.thumbnails = new FilePCv2[i];
		this.importDate = date;
	}


}

class AssetPCv1 extends AbstractIdentifiedPC {

	private URI origin;
	private Date importDate;
	//	private FilePCv1 file;
	//	private FilePCv1 sourceFile;
	private String s1, s2, s3; //-> new instance of 'file'.

	private FilePCv1[] thumbnails;
	private FilePCv1 selectedThumbnail;
	private boolean onlyFirstThumbGenerated;
	private UserPC creatingUser;
	private UserPC thumbnailSettingUser;


	private AssetPCv1(){
		// hidden default constructor for ZooDB.
	}


	public AssetPCv1(final long id, final URI request, final UserPC creatingUser, Date date, int i) {
		super(id);
		this.origin = request;
		this.creatingUser = creatingUser;
		this.onlyFirstThumbGenerated = true;
		this.importDate = date;
		this.thumbnails = new FilePCv1[i];
		
		this.s1 = "myS1";
		this.s2 = "myS2";
		this.s3 = "myS3";
	}


}

class FilePCv2 extends AbstractIdentifiedPC {

	private String storagePath;
	private String mimeType;
	private String originalFileName;


	private FilePCv2() {
		// hidden default constructor for ZooDB.
	}


	public String originalFileName() {
		zooActivateRead();
		return originalFileName;
	}


	public String mimeType() {
		zooActivateRead();
		return mimeType;
	}


	public String storagePath() {
		zooActivateRead();
		return storagePath;
	}


	public FilePCv2(final long id) {
		super(id);
	}


	public FilePCv2(final long id, final String storagePath, final String mimeType, final String originalFileName) {
		this(id);
		this.storagePath = storagePath;
		this.mimeType = mimeType;
		this.originalFileName = originalFileName;
	}
}

class FilePCv1 extends AbstractIdentifiedPC {

	private String storagePath;
	private String mimeType;
	private String originalFileName;


	private FilePCv1() {
		// hidden default constructor for ZooDB.
	}


	public FilePCv1(final long id) {
		super(id);
	}


	public FilePCv1(final long id, final String storagePath, final String mimeType, final String originalFileName) {
		this(id);
		this.storagePath = storagePath;
		this.mimeType = mimeType;
		this.originalFileName = originalFileName;
	}
}


class UserPC extends AbstractIdentifiedPC {

	private String name;

	private UserPC(){
		// hidden default constructor for ZooDB.
	}

	public UserPC(final long id, final String name) {
		super(id);
		this.name = name;
	}
}

