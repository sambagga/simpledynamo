package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
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
	static HashMap<String, String> sList;
	ContentResolver conRes;
	static String up;
	static int vote;
	static String[] cols = { KEY_S, VALUE_S };
	String node_id;

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
	public void ins(String key, String value) {
		Log.i("Provider ins " + selfPort, "key:" + key + ",value:" + value);
		try {
			FileOutputStream fos = pcontext.openFileOutput(key,
					Context.MODE_PRIVATE); // key is the filename
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			osw.write(value);
			osw.flush();
			osw.close();
		} catch (Exception e) {
			Log.i("chatStorage, fileCreate()", "Exception e = " + e);
		}
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
				} else if (genHash(key).compareTo(node_id) > 0
						&& genHash(key).compareTo(genHash(succ1)) < 0) {
					insertAt = succ1;
				}
			} else {
				if (genHash(key).compareTo(node_id) <= 0) {
					if (genHash(key).compareTo(genHash(succ2)) > 0) {
						insertAt = selfPort;
					} else if (genHash(key).compareTo(genHash(succ2)) < 0
							&& genHash(key).compareTo(genHash(succ1)) > 0)
						insertAt = succ2;
				} else if (genHash(key).compareTo(node_id) > 0
						&& genHash(key).compareTo(genHash(succ1)) < 0) {
					insertAt = succ1;
				}
			}
			Log.i("insert",insertAt);
			new Thread(new forClient(selfPort, Integer.parseInt(insertAt) * 2,
					5)).start();
			Thread.sleep(300);
			if(up == null)
				return null;
			Log.i("insert","up "+up);
			if (up.equals(insertAt))
				new Thread(new forClient(key + ";" + value,
						Integer.parseInt(insertAt) * 2, 2)).start();
			else {
				new Thread(new forClient(selfPort, Integer.parseInt(sList
						.get(insertAt)) * 2, 5)).start();
				Thread.sleep(300);
				if (up.equals(sList.get(insertAt)))
					new Thread(new forClient(key + ";" + value,
							Integer.parseInt(sList.get(insertAt)) * 2, 2))
							.start();
				else
					return null;
			}
		} catch (NoSuchAlgorithmException e) {

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}

	@Override
	public boolean onCreate() {
		pcontext = getContext();
		selfPort = getAVD();
		sList = new HashMap<String, String>();
		sList.put("5554", "5556");
		sList.put("5556", "5558");
		sList.put("5558", "5554");
		if (selfPort.equals("5554")) {
			succ1 = "5556";
			smallest = true;
		} else if (selfPort.equals("5556")) {
			succ1 = "5558";
			smallest = false;
		} else {
			succ1 = "5554";
			smallest = false;
		}
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
		// TODO Auto-generated method stub
		return null;
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
				if (type == 8) // gdump
					sendData.println("*" + msg);
				if (type == 9) // gdump reply
					sendData.println("(" + msg);
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
				while (true) {
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
						break;
					case '$': // write
						insertKey(recvMsg.substring(1));
						break;
					case '#': // vote request
						new Thread(new forClient(selfPort,
								Integer.parseInt(recvMsg.substring(1)) * 2, 4))
								.start();
						break;
					case '&': // request reply
						if (recvMsg.substring(1).equals(selfPort)
								|| recvMsg.substring(1).equals(succ1)
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
					// case '@': // get value for key
					// break;
					// case '*': // get local dump
					// break;
					// case '(': // get global dump
					// break;
					}
					recvSock.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Log.i(TAG, "I/O error occured when creating the socket!\n");
				e.printStackTrace();
			}

		}

		// insert key-value
		void insertKey(String msg) {
			StringTokenizer sTok = new StringTokenizer(msg, ";");
			String key = sTok.nextToken();
			String value = sTok.nextToken();
			vote = 0;
			getVotes();
			if (vote >= 2)
				ins(key, value);
		}

		void getVotes() {
			new Thread(new forClient(selfPort, Integer.parseInt(selfPort) * 2,
					3)).start();
			new Thread(new forClient(selfPort, Integer.parseInt(succ1) * 2, 3))
					.start();
			new Thread(new forClient(selfPort, Integer.parseInt(succ2) * 2, 3))
					.start();
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
