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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import javax.jdo.PersistenceManager;

import org.junit.Before;
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

	private URI uri;
	private Date date = new Date(12345678);

	public Test_039_SchemaEvolBug() throws URISyntaxException {
		uri = new URI("http://www.zoodb.org");
	}

	@Before
	public void before() {
		TestTools.closePM();
		TestTools.removeDb();
		TestTools.createDb();
	}

	@Test
	public void testIssue66() {
		//populate
		Object[] oids = init(false);

		//evolve
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema zs = ZooJdoHelper.schema(pm);
		ZooClass sFile = zs.getClass(FilePCv1.class.getName());
		sFile.rename(FilePCv2.class.getName());

		sFile.addField("originalFileName", String.class);

		ZooClass sAsset = zs.getClass(AssetPCv1.class.getName());
		sAsset.addField("file", sFile, 0);
		sAsset.addField("sourceFile", sFile, 0);

		ZooField zf1 = sAsset.getField("storagePath"); 
		ZooField zf2 = sAsset.getField("mimeType"); 
		ZooField zf3 = sAsset.getField("fileName"); 

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
			file.setValue("storagePath", assetHandle.getValue("storagePath"));
			file.setValue("mimeType", assetHandle.getValue("mimeType"));
			file.setValue("originalFileName", assetHandle.getValue("fileName"));

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

		AssetPCv2 a2 = (AssetPCv2) pm.getObjectById(oids[0]);
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

	private Object[] init(boolean createFile) {
		TestTools.createDb();
		TestTools.defineSchema(FilePCv1.class, AssetPCv1.class, IdPC.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Object[] oids = new Object[10];

		UserPC[] users = new UserPC[2];
		users[0] = new UserPC(333+0, "user"+0);
		users[1] = new UserPC(333+1, "user"+1);

		for (int i = 0; i < oids.length; i++) {
			AssetPCv1 a1 = new AssetPCv1(234, users[i%2], uri, date, 55);

			if (createFile) {
				a1.setFile(new FilePCv1(222+i, "sp" + i, "mt" + i));
			}
			pm.makePersistent(a1);

			oids[i] = pm.getObjectId(a1);
		}

		pm.currentTransaction().commit();
		TestTools.closePM();
		return oids;
	}

	private Object[] initProjectBranch() {
		TestTools.defineSchema(ProjectBranchPCv1.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Object[] oids = new Object[10];

		UserPC[] users = new UserPC[2];
		users[0] = new UserPC(333+0, "userPB"+0);
		users[1] = new UserPC(333+1, "userPB"+1);

		for (int i = 0; i < oids.length; i++) {
			ProjectBranchPCv1 a1 = new ProjectBranchPCv1(234, "PB"+i, users[i%2]);
			pm.makePersistent(a1);
			oids[i] = pm.getObjectId(a1);
		}

		pm.currentTransaction().commit();
		TestTools.closePM();
		return oids;
	}

	@Test
	public void testIssue66NoReflection() {
		//populate
		Object[] oids = init(false);

		//evolve
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema zs1 = ZooJdoHelper.schema(pm);
		ZooClass sFile1 = zs1.getClass(FilePCv1.class.getName());
		sFile1.rename(FilePCv2.class.getName());

		sFile1.addField("originalFileName", String.class);

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

		ZooField zf1 = sAsset.getField("storagePath"); 
		ZooField zf2 = sAsset.getField("mimeType"); 
		ZooField zf3 = sAsset.getField("fileName"); 

		//evolve 
		Iterator<ZooHandle> assetHandleIterator = sAsset.getHandleIterator(false);
		while (assetHandleIterator.hasNext()) {
			final ZooHandle assetHandle = assetHandleIterator.next();
			final FilePCv2 file = new FilePCv2(
					123,
					(String) assetHandle.getValue("storagePath"),
					(String) assetHandle.getValue("mimeType"),
					(String) assetHandle.getValue("fileName"));
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

		AssetPCv2 a2 = (AssetPCv2) pm.getObjectById(oids[0]);
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


	@Test
	public void testFull() {
		//populate
		init(true);

		PersistenceManager pm = TestTools.openPM();
		applyInternal(pm);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		int n = 0;
		@SuppressWarnings("unchecked")
		Collection<AssetPCv2> c = (Collection<AssetPCv2>) pm.newQuery(AssetPCv2.class).execute();
		for (AssetPCv2 a: c) {
			assertNotNull(a.getFile());
			assertNotNull(a.getFile().originalFileName());
			n++;
		}
		assertEquals(10, n);

		pm.currentTransaction().rollback();
		TestTools.closePM();

	}

	@Test
	public void testFullWithProjectBranch() {
		//populate
		init(true);
		initProjectBranch();

		ZooJdoProperties p = TestTools.getProps();
		p.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(p);
		applyInternal(pm);
		ZooSchema schema = ZooJdoHelper.schema(pm);
		ZooClass pbClass = schema.getClass(ProjectBranchPCv1.class.getName());
		pbClass.rename(ProjectBranchPCv2.class.getName());
		//pbClass.addField("bgImage", BackgroundImagePCv2.class);
		//Iterator<ZooHandle> hi = pbClass.getHandleIterator(true);
		//while (hi.hasNext()) {
		//	//this was an attempt to enforce evolution of objects. It resulted in
		//	//an NPE in the Deserializer when accessing the class-map with cls=null.
		//	hi.next();
		//}
		//schema.addClass(BackgroundImagePCv2.class);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		int n = 0;
		@SuppressWarnings("unchecked")
		Collection<ProjectBranchPCv2> cPB = (Collection<ProjectBranchPCv2>) 
				pm.newQuery(ProjectBranchPCv2.class).execute();
		for (ProjectBranchPCv2 a: cPB) {
			assertNotNull(a.getName());
			assertNull(a.getBackgroundImage());
			n++;
		}
		assertEquals(10, n);

		n = 0;
		@SuppressWarnings("unchecked")
		Collection<AssetPCv2> c = (Collection<AssetPCv2>) pm.newQuery(AssetPCv2.class).execute();
		for (AssetPCv2 a: c) {
			assertNotNull(a.getFile());
			assertNotNull(a.getFile().originalFileName());
			n++;
		}
		assertEquals(10, n);

		pm.currentTransaction().rollback();
		TestTools.closePM();

	}

	private void applyInternal(PersistenceManager pm) { 

		final String fileFieldName = "file";
		final String sourceFileFieldName = "sourceFile";
		final String storagePathFieldName = "storagePath";
		final String mimeTypeFieldName = "mimeType";
		final String storagePathFieldNameF = "storagePath";
		final String mimeTypeFieldNameF = "mimeType";
		final String fileNameFieldName = "fileName";
		final String originalFileNameFieldName = "originalFileName";
		final String idFieldName = "id";

		pm.currentTransaction().begin();
		final ZooSchema schema = ZooJdoHelper.schema(pm);

		// rename the ThumbnailPC class to FilePC
		// ThumbnailPC.class no longer exists in the code base, so we reference it by name string.
		//Evolve0to1.logger.info("Renaming ThumbnailPC to FilePC");
		final ZooClass filePCclass = schema.getClass(FilePCv1.class.getName());
		filePCclass.rename(FilePCv2.class.getName());

		// add field originalFileName to FilePC
		//Evolve0to1.logger.info("Adding field to FilePC");
		filePCclass.addField(originalFileNameFieldName, String.class);

		// add fields of type FilePC to AssetPC: file, sourceFile
		//Evolve0to1.logger.info("Adding fields to AssetPC");
		final ZooClass assetPCclass = schema.getClass(AssetPCv1.class.getName());
		assetPCclass.addField(fileFieldName, FilePCv2.class);
		assetPCclass.addField(sourceFileFieldName, FilePCv2.class);

		// get id generator
		//Evolve0to1.logger.info("Fetching IdGenerator");
		//IdGenerator.init(pm);
		//final IdGenerator idGenerator = IdGenerator.instance();
		long idGen = 123456;

		//Evolve0to1.logger.info("Fetching classes and fields");

		// get IdPC class and fields
		final ZooClass idPCclass = schema.getClass(IdPC.class);
		final ZooField idPCidField = idPCclass.getField(idFieldName);
		final ZooField idPCidentifeeField = idPCclass.getField("identifee");

		// get AssetPC fields
		final ZooField assetPCfileField = assetPCclass.getField(fileFieldName);
		final ZooField assetPCsourcefileField = assetPCclass.getField(sourceFileFieldName);

		// get FilePC fields
		final ZooField filePCidField = filePCclass.getField(idFieldName);
		final ZooField filePCstoragepathField = filePCclass.getField(storagePathFieldName);
		final ZooField filePCmimetypeField = filePCclass.getField(mimeTypeFieldName);
		final ZooField filePCoriginalfilenameField = filePCclass.getField(originalFileNameFieldName);

		// populate the FilePC attributes of AssetPC
		//Evolve0to1.logger.info("Populating new fields in AssetPC with FilePC instances");
		final Iterator<ZooHandle> assetHandleIterator = assetPCclass.getHandleIterator(false);
		while (assetHandleIterator.hasNext()) {
			final long id = idGen++;

			final ZooHandle assetHandle = assetHandleIterator.next();

			//		      final FilePC file = new FilePC(
			//		          id,
			//		          (String) assetHandle.getValue(storagePathFieldName),
			//		          (String) assetHandle.getValue(mimeTypeFieldName),
			//		          (String) assetHandle.getValue(fileNameFieldName));
			//		      final FilePC sourceFile = null;
			final ZooHandle file = filePCclass.newInstance();
			filePCidField.setValue(file, id);
			filePCstoragepathField.setValue(file, assetHandle.getValue(storagePathFieldNameF));
			filePCmimetypeField.setValue(file, assetHandle.getValue(mimeTypeFieldNameF));
			filePCoriginalfilenameField.setValue(file, assetHandle.getValue(fileNameFieldName));

			final ZooHandle idPC = idPCclass.newInstance();
			idPCidField.setValue(idPC, id);
			idPCidentifeeField.setValue(idPC, file);

			assetPCfileField.setValue(assetHandle, file);
			assetPCsourcefileField.setValue(assetHandle, null);
		}

		// remove fields from AssetPC: storagePath, mimeType, fileName
		//Evolve0to1.logger.info("Removing fields from AssetPC");
		assetPCclass.getField(storagePathFieldNameF).remove();
		assetPCclass.getField(mimeTypeFieldNameF).remove();
		assetPCclass.getField(fileNameFieldName).remove();

		assetPCclass.rename(AssetPCv2.class.getName());
		//Evolve0to1.logger.info("Done!");
	}


	//	@Test
	//	public void testOrig() {
	//		//renameOrigToTestClasses();
	//		PersistenceManager pm = TestTools.openPM("IdeaGarden-Backend.db");
	//		applyInternal(pm);
	//		pm.currentTransaction().rollback();
	//		TestTools.closePM();
	//	}

	private void renameOrigToTestClasses() {
		PersistenceManager pm = TestTools.openPM("IdeaGarden-Backend.db");
		pm.currentTransaction().begin();

		ZooSchema schema = ZooJdoHelper.schema(pm);
		schema.getClass("eu.ideagarden.backend.persistent.ThumbnailPC").rename(FilePCv1.class.getName());
		schema.getClass("eu.ideagarden.backend.persistent.AssetPC").rename(AssetPCv1.class.getName());
		schema.getClass("eu.ideagarden.backend.persistent.UserPC").rename(UserPC.class.getName());
		schema.getClass("eu.ideagarden.backend.persistent.IdPC").rename(IdPC.class.getName());
		schema.getClass("eu.ideagarden.backend.persistent.AbstractPC").rename(AbstractPC.class.getName());
		schema.getClass("eu.ideagarden.backend.persistent.AbstractIdentifiedPC").rename(AbstractIdentifiedPC.class.getName());

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


@SuppressWarnings("unused")
class AssetPCv1 extends AbstractIdentifiedPC {

	private URI origin;
	private Date importDate;
	//	private FilePCv1 file;
	//	private FilePCv1 sourceFile;
	private String storagePath, mimeType, fileName; //-> new instance of 'file'.

	private FilePCv1[] thumbnails;
	private FilePCv1 selectedThumbnail;
	private boolean onlyFirstThumbGenerated;
	private UserPC creatingUser;
	private UserPC thumbnailSettingUser;


	private AssetPCv1(){
		// hidden default constructor for ZooDB.
	}


	public void setFile(FilePCv1 file) {
		selectedThumbnail = file;
	}


	public AssetPCv1(final long id, UserPC creatingUser, final URI request, Date date, int i) {
		super(id);
		this.origin = request;
		this.creatingUser = creatingUser;
		this.onlyFirstThumbGenerated = true;
		this.importDate = date;
		this.thumbnails = new FilePCv1[i];

		this.storagePath = "myS1";
		this.mimeType = "myS2";
		this.fileName = "myS3";
	}
}


@SuppressWarnings("unused")
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


	public AssetPCv2(final long id, UserPC creatingUser, final URI request, Date date, int i) {
		super(id);
		this.origin = request;
		this.creatingUser = creatingUser;
		this.onlyFirstThumbGenerated = true;
		this.thumbnails = new FilePCv2[i];
		this.importDate = date;
	}
}


class FilePCv2 extends AbstractIdentifiedPC {

	private String storagePath;
	private String mimeType;
	private String originalFileName;


	@SuppressWarnings("unused")
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

@SuppressWarnings("unused")
class FilePCv1 extends AbstractIdentifiedPC {

	private String storagePath;
	private String mimeType;
	//private String originalFileName;


	private FilePCv1() {
		// hidden default constructor for ZooDB.
	}


	public FilePCv1(final long id) {
		super(id);
	}


	public FilePCv1(final long id, final String storagePath, final String mimeType) {
		this(id);
		this.storagePath = storagePath;
		this.mimeType = mimeType;
		//this.originalFileName = originalFileName;
	}
}


class IdPC extends AbstractIdentifiedPC {

	private AbstractIdentifiedPC identifee;

	@SuppressWarnings("unused")
	private IdPC() {
		// hidden default constructor for ZooDB
	}

	public IdPC(final long id) {
		super(id);
	}

	public void setIdentfee(final AbstractIdentifiedPC identifee) {
		this.zooActivateWrite();
		this.identifee = identifee;
	}

	public AbstractIdentifiedPC getIdentifee() {
		this.zooActivateRead();
		return this.identifee;
	}

	public void removeIdentifee() {
		this.zooActivateWrite();
		this.identifee = null;
	}
}

class UserPC extends AbstractIdentifiedPC {

	private String name;

	@SuppressWarnings("unused")
	private UserPC(){
		// hidden default constructor for ZooDB.
	}

	public UserPC(final long id, final String name) {
		super(id);
		this.name = name;
	}


	public String getName() {
		this.zooActivateRead();
		return this.name;
	}


	public void setName(final String name) {
		this.zooActivateWrite();
		this.name = name;
	}
}


class BackgroundImagePCv2 extends AbstractPC {
	private int i1;
	private int i2;
	private int coordX, coordY;

}

class ProjectBranchPCv1 extends AbstractIdentifiedPC {

	private String name;
	private UserPC creator;
	private Date creationDate;
	private Date lastUpdateDate;
	//  private ProjectSnapshotPC initialState;
	//  private ProjectBranchPC sourceBranch;
	//  private Map<Long, ProjectSnapshotPC> snapshots;
	//  private ProjectSnapshotPC currentState;
	//  private ProjectSnapshotPC baseSnapshot;


	protected ProjectBranchPCv1() {
		// hidden default constructor for ZooDB
	}

	public ProjectBranchPCv1(final long id, final String name, final UserPC creator) {
		//final IdPC currentStateId) {
		//, final ProjectSnapshotPC initialState, final ProjectBranchPC sourceBranch) {
		super(id);
		this.name = name;
		this.creator = creator;
		this.creationDate = Calendar.getInstance().getTime();
		this.lastUpdateDate = this.creationDate;
		//	    this.snapshots = new HashMap<Long, ProjectSnapshotPC>();
		//	    this.currentState = new ProjectSnapshotPC(currentStateId.getId(), creator, "current state", null);
		//	    currentStateId.setIdentfee(this.currentState);
		//	    this.initialState = initialState;
		//	    this.sourceBranch = sourceBranch;
	}


	public String getName() {
		this.zooActivateRead();
		return this.name;
	}


	public UserPC getCreator() {
		this.zooActivateRead();
		return this.creator;
	}


	public Date getCreationDate() {
		this.zooActivateRead();
		return this.creationDate;
	}



	public Date getLastUpdateDate() {
		this.zooActivateRead();
		return this.lastUpdateDate;
	}


	/**
	 * Sets lastUdate date to now.
	 */
	public void setLastUpdateDate() {
		this.zooActivateWrite();
		this.lastUpdateDate = Calendar.getInstance().getTime();
	}



}

class ProjectBranchPCv2 extends AbstractIdentifiedPC {

	private String name;
	private UserPC creator;
	private Date creationDate;
	private Date lastUpdateDate;
	//  private ProjectSnapshotPC initialState;
	//  private ProjectBranchPC sourceBranch;
	//  private Map<Long, ProjectSnapshotPC> snapshots;
	//  private ProjectSnapshotPC currentState;
	//  private ProjectSnapshotPC baseSnapshot;
	private BackgroundImagePCv2 bgImage;


	protected ProjectBranchPCv2() {
		// hidden default constructor for ZooDB
	}

	public ProjectBranchPCv2(final long id, final String name, final UserPC creator) {
		super(id);
		this.name = name;
		this.creator = creator;
		this.creationDate = Calendar.getInstance().getTime();
		this.lastUpdateDate = this.creationDate;
	}


	public BackgroundImagePCv2 getBackgroundImage() {
		zooActivateRead();
		return bgImage;
	}

	public String getName() {
		this.zooActivateRead();
		return this.name;
	}


	public UserPC getCreator() {
		this.zooActivateRead();
		return this.creator;
	}


	public Date getCreationDate() {
		this.zooActivateRead();
		return this.creationDate;
	}



	public Date getLastUpdateDate() {
		this.zooActivateRead();
		return this.lastUpdateDate;
	}


	/**
	 * Sets lastUdate date to now.
	 */
	 public void setLastUpdateDate() {
		 this.zooActivateWrite();
		 this.lastUpdateDate = Calendar.getInstance().getTime();
	 }

	 //	  public ProjectSnapshotPC getInitialState() {
	 //	    this.zooActivateRead();
	 //	    return this.initialState;
	 //	  }
	 //
	 //
	 //	  public Map<Long, ProjectSnapshotPC> getSnapshots() {
	 //	    this.zooActivateWrite();
	 //	    return this.snapshots;
	 //	  }
	 //
	 //
	 //	  public ProjectSnapshotPC getCurrentState() {
	 //	    this.zooActivateWrite();
	 //	    return this.currentState;
	 //	  }
	 //
	 //
	 //	  public ProjectBranchPC getSourceBranch() {
	 //	    this.zooActivateRead();
	 //	    return this.sourceBranch;
	 //	  }
	 //
	 //
	 //	  public ProjectSnapshotPC getBaseSnapshot() {
	 //	    this.zooActivateRead();
	 //	    return this.baseSnapshot;
	 //	  }
	 //
	 //
	 //	  public void setBaseSnapshot(final ProjectSnapshotPC baseSnapshot) {
	 //	    this.zooActivateWrite();
	 //	    this.baseSnapshot = baseSnapshot;
	 //	  }
	 //
	 //
	 //	  @Override
	 //	  public ProjectBranchPC copy(final IdentityHashMap<AbstractPC, AbstractPC> copiedObjects) {
	 //	    if (copiedObjects.containsKey(this)) {
	 //	      return (ProjectBranchPC) copiedObjects.get(this);
	 //	    }
	 //
	 //	    final ProjectBranchPC copy = new ProjectBranchPC();
	 //	    copiedObjects.put(this, copy);
	 //	    this.copyInternal(copy, copiedObjects);
	 //	    return copy;
	 //	  }
	 //
	 //	  @Override
	 //	  protected void copyInternal(final AbstractPC abstractCopy, final IdentityHashMap<AbstractPC, AbstractPC> copiedObjects) {
	 //	    final ProjectBranchPC copy = (ProjectBranchPC) abstractCopy;
	 //	    this.zooActivateRead();
	 //	    copy.name = this.name;
	 //	    copy.creationDate = this.creationDate;
	 //	    copy.lastUpdateDate = this.lastUpdateDate;
	 //	    copy.creator = this.creator.copy(copiedObjects);
	 //	    if (this.baseSnapshot != null) {
	 //	      copy.baseSnapshot = this.baseSnapshot.copy(copiedObjects);
	 //	    }
	 //	    copy.currentState = this.currentState.copy(copiedObjects);
	 //	    if (this.initialState != null) {
	 //	      copy.initialState = this.initialState.copy(copiedObjects);
	 //	    }
	 //	    copy.snapshots = new HashMap<Long, ProjectSnapshotPC>();
	 //	    for (final Entry<Long, ProjectSnapshotPC> entry : this.snapshots.entrySet()) {
	 //	      copy.snapshots.put(entry.getKey(), entry.getValue().copy(copiedObjects));
	 //	    }
	 //	    if (this.sourceBranch != null) {
	 //	      copy.sourceBranch = this.sourceBranch.copy(copiedObjects);
	 //	    }
	 //	    super.copyInternal(copy, copiedObjects);
	 //	  }
}