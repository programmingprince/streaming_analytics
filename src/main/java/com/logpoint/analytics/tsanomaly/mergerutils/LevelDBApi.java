package com.logpoint.analytics.tsanomaly.mergerutils;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.logpoint.analytics.tsanomaly.AnomalyEngine;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.immunesecurity.shared.lib.Log;
import com.logpoint.analytics.tsanomaly.AnomalyModel;

public class LevelDBApi {
	private DB leveldb;
	String path;

	public LevelDBApi() {
		path = getStorageBasePath() + "/tsanomaly";
		Options options = new Options();
		try {
			leveldb = factory.open(new File(path), options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void saveModel(String key, AnomalyModel anomalyModel) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(out);
			os.writeObject(anomalyModel);
			leveldb.put(key.getBytes(), out.toByteArray());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public AnomalyModel loadModel(String key) {
		AnomalyModel anomalyModel = null;
		byte[] data = leveldb.get(key.getBytes());
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(in);
			anomalyModel = (AnomalyModel) is.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return anomalyModel;
	}

	public boolean isPersisted(String key) {
		byte[] data = leveldb.get(key.getBytes());
		if (data == null) {
			return false;
		} else {
			return true;
		}
	}

	public void saveLastTriggered(String key, Long value) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(out);
			os.writeObject(value);
			leveldb.put(key.getBytes(), out.toByteArray());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Long loadLastTriggered(String key) {
		Long lastTriggered = null;
		byte[] data = leveldb.get(key.getBytes());
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(in);
			lastTriggered = (Long) is.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lastTriggered;
	}

	public void persistLastTriggeredMap() {
		for (Map.Entry<String, Long> entry : AnomalyEngine.lastTriggeredMap
				.entrySet()) {
			Log.debug("persisted last triggerd timestamp:" + entry.getValue()
					+ " for lifeID: " + entry.getKey());
			saveLastTriggered(entry.getKey(), entry.getValue());
		}
	}

	public void persistModel() {
		for (Map.Entry<String, AnomalyModel> entry : AnomalyEngine.anomalyModelMap
				.entrySet()) {
			saveModel(entry.getKey().toString(), entry.getValue());
		}
	}

	public DB getLeveldb() {
		return this.leveldb;
	}

	private static String getStorageBasePath() {
		String STORAGE_BASE_PATH;
		String liHome = System.getenv("LOGINSPECT_HOME");

		// for running locally
		if (liHome == null) {
			STORAGE_BASE_PATH = "/opt/immune/storage";
		} else {
			STORAGE_BASE_PATH =
					new StringBuffer(liHome).append("/storage").toString();
		}
		return STORAGE_BASE_PATH;
	}

	public void closeDB() {
		try {
			leveldb.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
