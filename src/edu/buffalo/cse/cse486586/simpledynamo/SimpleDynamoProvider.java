package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	Context pcontext;
	private static final String KEY_S = "key";
	private static final String VALUE_S = "value";

	static String selfPort;
	static String succ1;
	static String succ2;
	static Boolean smallest;
	static String[][] queryValue;
	static HashMap<String, String> sList;
	ContentResolver conRes;
	static String up;
	static int vote;
	static String[] cols = { KEY_S, VALUE_S };
	String node_id;
	static int nquery;
	static int max;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String[] savFiles = pcontext.fileList();
		for (int i = 0; i < savFiles.length; i++) {
			pcontext.deleteFile(savFiles[i]);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	// to locally insert the key, value pair in internal storage
	public void ins(String key, String value, String mver) {
		try {
			int version;
			if (mver == null) {
				File file = pcontext.getFileStreamPath(key);
				int oldversion;
				if (file.exists()) {
					FileInputStream fin = pcontext.openFileInput(key);
					InputStreamReader inpReader = new InputStreamReader(fin);
					BufferedReader br = new BufferedReader(inpReader);
					// Fill the buffer with data from file
					String valver = br.readLine();
					StringTokenizer sTok = new StringTokenizer(valver, ";");
					String val = sTok.nextToken();
					String ver = sTok.nextToken();
					oldversion = Integer.parseInt(ver);
					if (val.equals(value))
						version = oldversion;
					else
						version = oldversion + 1;
					fin.close();
				} else
					version = 0;
			} else {
				version = Integer.parseInt(mver);
			}
			Log.i("Provider ins " + selfPort, "key:" + key + ",value:" + value
					+ ",version:" + version);
			FileOutputStream fos = pcontext.openFileOutput(key,
					Context.MODE_PRIVATE); // key is the filename
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			osw.write(value + ";" + version);
			osw.flush();
			osw.close();
			fos.close();
		} catch (Exception e) {
			Log.i("chatStorage, fileCreate()", "Exception e = " + e);
		}
	}

	// local query for value corresponding to key
	String quer(String selection) {
		String fname = selection;
		String valver;
		Log.i("Provider quer", "key:" + selection);
		File file = pcontext.getFileStreamPath(fname);
		if (file.exists()) {
			try {
				FileInputStream fin = pcontext.openFileInput(fname);
				InputStreamReader inpReader = new InputStreamReader(fin);
				BufferedReader br = new BufferedReader(inpReader);
				// Fill the buffer with data from file
				valver = br.readLine();
			} catch (Exception e) {
				Log.i("chatStorage, readFile()", "Exception e = " + e);
				valver = null;
			}
		} else
			valver = null;
		Log.i("Provider quer", "value-version:" + valver);
		return valver;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = (String) values.get(KEY_S);
		String value = (String) values.get(VALUE_S);
		String insertAt = null;
		Log.i("Provider insert", "key:" + key + ",value:" + value);
		try {
			if (smallest == true) {
				// the one with largest hash ultimately stored in smallest node
				if (genHash(key).compareTo(node_id) <= 0
						|| genHash(key).compareTo(genHash(succ2)) > 0) {
					insertAt = selfPort;
				} else if (genHash(key).compareTo(node_id) > 0) {
					insertAt = succ1;
				}
			} else {
				if (genHash(key).compareTo(genHash(selfPort)) <= 0) {
					if (genHash(key).compareTo(genHash(succ2)) > 0) {
						insertAt = selfPort;
					} else
						insertAt = succ2;
				} else if (genHash(key).compareTo(node_id) > 0) {
					insertAt = succ1;
				}
			}

			Log.i("insertAt", insertAt + " from " + selfPort);
			new Thread(new forClient(selfPort, Integer.parseInt(insertAt) * 2,
					5)).start();
			Thread.sleep(250);
			if (up != null && up.equals(insertAt)) {
				Log.i("insert", "up " + up);
				new Thread(new forClient(key + ";" + value,
						Integer.parseInt(insertAt) * 2, 2)).start();
			} else {
				Log.i("Try inserting at", sList.get(insertAt));
				new Thread(new forClient(selfPort, Integer.parseInt(sList
						.get(insertAt)) * 2, 5)).start();
				Thread.sleep(250);
				if (up != null && up.equals(sList.get(insertAt))) {
					Log.i("insert", "up " + up);
					new Thread(new forClient(key + ";" + value,
							Integer.parseInt(sList.get(insertAt)) * 2, 2))
							.start();
				} else {
					Log.i("insert fail!", selfPort);
					return null;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		pcontext = getContext();
		selfPort = getAVD();
		sList = new HashMap<String, String>();
		sList.put("5554", "5558");
		sList.put("5556", "5554");
		sList.put("5558", "5556");
		smallest = false;
		if (selfPort.equals("5556"))
			smallest = true;
		succ1 = sList.get(selfPort);
		succ2 = sList.get(succ1);
		try {
			node_id = genHash(selfPort);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conRes = pcontext.getContentResolver();
		Thread th = new forServer();
		th.start();

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		String key = selection;
		String queryAt = null;
		nquery = 0;
		queryValue = new String[3][2];
		MatrixCursor mcur = new MatrixCursor(cols);
		if (selectionArgs != null && selectionArgs.length > 0) {
			if (selectionArgs[0].equals("ldump")) {
				String[] savFiles = pcontext.fileList(); // get keys
				for (int i = 0; i < savFiles.length; i++) {
					String valver = quer(savFiles[i]);
					StringTokenizer sTok = new StringTokenizer(valver, ";");
					String value = sTok.nextToken();
					String version = sTok.nextToken();
					String[] row = { savFiles[i], value };
					mcur.addRow(row); // query for each key from local storage
				}
			}
		} else {
			Log.i("Provider query " + selfPort, "key:" + key);
			try {
				String valver = quer(key);
				if (valver != null) {
					StringTokenizer sTok = new StringTokenizer(valver, ";");
					String value = sTok.nextToken();
					String version = sTok.nextToken();
					queryValue[nquery][0] = value;
					queryValue[nquery][1] = version;
					nquery++;
					Log.i("queryAt", succ1);
					new Thread(new forClient(key + ";" + selfPort,
							Integer.parseInt(succ1) * 2, 1)).start();
					Log.i("queryAt", succ2);
					new Thread(new forClient(key + ";" + selfPort,
							Integer.parseInt(succ2) * 2, 1)).start();
					Thread.sleep(600);
					if (nquery > 1) {
						max = 0;
						for (int i = 1; i < nquery; i++) {
							if (Integer.parseInt(queryValue[max][1]) < Integer
									.parseInt(queryValue[i][1]))
								max = i;
						}
						String[] row = { selection, queryValue[max][0] };
						mcur.addRow(row);
					} else
						return null;
				} else
					return null;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		return mcur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	// to identify AVD executing the app
	public String getAVD() {
		TelephonyManager tel = (TelephonyManager) getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		return portStr;
	}

	// client thread to send messages to specific port
	class forClient extends Thread {
		String msg, TAG = "forClient";
		int type, port;

		forClient(String msg, int port, int msgType) {
			this.msg = msg;
			this.port = port;
			type = msgType;
		}

		public void run() {
			Socket clSock;
			try {
				// Log.i("Client Thread Provider", msg);
				// connect to server
				clSock = new Socket("10.0.2.2", port);
				// Log.i("Sending Message type" + type + "=", msg);
				// send the message to server
				PrintWriter sendData = new PrintWriter(clSock.getOutputStream());
				if (type == 1) // read
					sendData.println("%" + msg);
				if (type == 2) // write
					sendData.println("$" + msg);
				if (type == 3) // vote request
					sendData.println("#" + msg);
				if (type == 4) // vote request reply
					sendData.println("&" + msg);
				if (type == 5) // check if up
					sendData.println("!" + msg);
				if (type == 6) // to tell it is up
					sendData.println("^" + msg);
				if (type == 7) // query response
					sendData.println("@" + msg);
				sendData.flush();
				sendData.close();
				clSock.close();
				// Log.i("Message sent=", msg);

			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				Log.i(TAG, "Number format Exception!\n");
				e.printStackTrace();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				Log.i(TAG, "Unknown Host Exception!\n");
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Log.i(TAG, "I/O error occured when creating the socket!\n");
				e.printStackTrace();
			} catch (SecurityException e) {
				Log.i(TAG, "Security Exception!\n");
				e.printStackTrace();
			}

		}
	}// end of client thread

	// server thread
	class forServer extends Thread {

		String TAG = "forServer";

		public void run() {
			try {
				// open connection on port 10000
				ServerSocket serSock = new ServerSocket(10000);
				SimpleDynamoActivity.socket = serSock;
				Thread rec = new recovery();
				rec.start();
				while (!SimpleDynamoActivity.shutDown) {
					// listen for client
					Socket recvSock = serSock.accept();
					// Log.i("Connection", "Accepted");
					// get the message
					InputStreamReader readStream = new InputStreamReader(
							recvSock.getInputStream());
					BufferedReader recvInp = new BufferedReader(readStream);
					// Log.i("Reader", "Initialized");
					String recvMsg = recvInp.readLine();
					// Log.i("Received Message Main:", recvMsg);
					// recognise message type
					switch (recvMsg.charAt(0)) {
					case '%': // read
						Thread quer = new queryKey(recvMsg.substring(1));
						quer.start();
						break;
					case '$': // write
						Thread ins = new insertKey(recvMsg.substring(1));
						ins.start();
						break;
					case '#': // vote request
						makeReplica(recvMsg.substring(1));
						break;
					case '&': // request reply
						if (recvMsg.substring(1).equals(succ1)
								|| recvMsg.substring(1).equals(succ2))
							vote++;
						break;
					case '!': // check if up
						new Thread(new forClient(selfPort,
								Integer.parseInt(recvMsg.substring(1)) * 2, 6))
								.start();
						break;
					case '^': // to tell it is up
						up = recvMsg.substring(1);
						break;
					case '@': // get value for key
						String valver = recvMsg.substring(1);
						if (!valver.equals("null")) {
							Log.i("Get value for key", valver);
							StringTokenizer sTok = new StringTokenizer(valver,
									";");
							String value = sTok.nextToken();
							String version = sTok.nextToken();
							queryValue[nquery][0] = value;
							queryValue[nquery][1] = version;
							nquery++;
						}
						break;
					// case '*': // get local dump
					// break;
					// case '(': // get global dump
					// break;
					}
					recvSock.close();
				}
				serSock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Log.i(TAG, "I/O error occured when creating the socket!\n");
				e.printStackTrace();
			}

		}

		// insert key-value
		class insertKey extends Thread {
			String msg;

			insertKey(String msg) {
				this.msg = msg;
			}

			public void run() {
				StringTokenizer sTok = new StringTokenizer(msg, ";");
				String key = sTok.nextToken();
				String value = sTok.nextToken();
				vote = 1;
				msg = msg.concat(";" + selfPort + ";1");
				getVotes(msg);
				if (vote >= 2)
					ins(key, value, null);
			}
		}// end of insertKey thread

		// query key
		class queryKey extends Thread {
			String msg;

			queryKey(String msg) {
				this.msg = msg;
			}

			public void run() {
				StringTokenizer sTok = new StringTokenizer(msg, ";");
				String key = sTok.nextToken();
				String port = sTok.nextToken();
				vote = 1;
				msg = msg.concat(";" + selfPort + ";2");
				getVotes(msg);
				if (vote >= 2) {
					String valver = quer(key);
					Log.i("Server queryKey", "key:" + key + ",port:" + port
							+ ",valver:" + valver);
					new Thread(new forClient(valver,
							Integer.parseInt(port) * 2, 7)).start();
				}
			}
		}// end of queryKey thread

		void getVotes(String msg) {
			new Thread(new forClient(msg, Integer.parseInt(succ1) * 2, 3))
					.start();
			new Thread(new forClient(msg, Integer.parseInt(succ2) * 2, 3))
					.start();
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		void makeReplica(String msg) {
			StringTokenizer sTok = new StringTokenizer(msg, ";");
			String key = sTok.nextToken();
			String value = sTok.nextToken();
			String port = sTok.nextToken();
			String flag = sTok.nextToken();
			Log.i("Vote from " + selfPort, "For " + port);
			if (flag.equals("1"))
				ins(key, value, null);
			new Thread(new forClient(selfPort, Integer.parseInt(port) * 2, 4))
					.start();
		}

		class recovery extends Thread {

			public void run() {
				for (int i = 0; i < SimpleDynamoActivity.TEST_CNT; i++) {
					String key = "" + i;

					Cursor resultCursor = query(SimpleDynamoActivity.mUri,
							null, key, null, null);
					if (resultCursor == null) {
						Log.e(TAG, "Result null");
						break;
					}

					int keyIndex = resultCursor.getColumnIndex(KEY_S);
					int valueIndex = resultCursor.getColumnIndex(VALUE_S);
					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e(TAG, "Wrong number of rows");
						resultCursor.close();
					}

					final String returnKey = resultCursor.getString(keyIndex);
					final String returnValue = resultCursor
							.getString(valueIndex);
					String returnVersion = queryValue[max][1];
					ins(returnKey, returnValue, returnVersion);

					resultCursor.close();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}// end of recovery thread
	}// end of server thread
}
