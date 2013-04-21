package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;

import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	public static final int TEST_CNT = 20;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String TAG = SimpleDynamoActivity.class.getName();
	Handler handle = new Handler();
	TextView tv;
	public static Uri mUri;
	ContentResolver conRes;
	public volatile static Boolean shutDown = false;
	public static ServerSocket socket;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());

		mUri = buildUri("content",
				"edu.buffalo.cse.cse486586.simpledynamo.provider");
		conRes = getContentResolver();
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

	// build Uri
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public void Put1(View v) {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		Uri temp;
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, Integer.toString(i));
			cv[i].put(VALUE_FIELD, "Put1" + Integer.toString(i));
		}
		try {
			for (int i = 0; i < TEST_CNT; i++) {
				temp = conRes.insert(mUri, cv[i]);
				if(temp==null)
				{
					Log.e(TAG, "Quorum not established!");
					handle.post(new Runnable() {
						public void run() {
							tv.append("Quorum not established!\n");
						}
					});
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Log.e(TAG, e.toString());
		}
	}

	public void Put2(View v) {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		Uri temp;
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, Integer.toString(i));
			cv[i].put(VALUE_FIELD, "Put2" + Integer.toString(i));
		}
		try {
			for (int i = 0; i < TEST_CNT; i++) {
				temp = conRes.insert(mUri, cv[i]);
				if(temp==null)
				{
					Log.e(TAG, "Quorum not established!");
					handle.post(new Runnable() {
						public void run() {
							tv.append("Quorum not established!\n");
						}
					});
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Log.e(TAG, e.toString());
		}
	}

	public void Put3(View v) {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		Uri temp;
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, Integer.toString(i));
			cv[i].put(VALUE_FIELD, "Put3" + Integer.toString(i));
		}
		try {
			for (int i = 0; i < TEST_CNT; i++) {
				temp = conRes.insert(mUri, cv[i]);
				if(temp==null)
				{
					Log.e(TAG, "Quorum not established!");
					handle.post(new Runnable() {
						public void run() {
							tv.append("Quorum not established!\n");
						}
					});
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Log.e(TAG, e.toString());
		}
	}

	// LDump
	public void LDump(View v) {
		String[] selArgs = { "ldump" };
		// get locally stored key, value pairs of DHT
		Cursor resultCursor = conRes.query(mUri, null, null, selArgs, null);
		int keyIndex = resultCursor.getColumnIndex("key");
		int valueIndex = resultCursor.getColumnIndex("value");

		// cursor traversal
		resultCursor.moveToFirst();
		while (!resultCursor.isAfterLast()) {
			final String key = resultCursor.getString(keyIndex);
			final String value = resultCursor.getString(valueIndex);
			handle.post(new Runnable() {
				public void run() {
					tv.append("key:" + key + ",value:" + value + "\n");
				}
			});

			resultCursor.moveToNext();
		}
		resultCursor.close();
	}
	
	public void Get(View v){
		try {
			for (int i = 0; i < TEST_CNT; i++) {
				String key = ""+i;

				Cursor resultCursor = conRes.query(mUri, null,
						key, null, null);
				if (resultCursor == null) {
					Log.e(TAG, "Result null");
					handle.post(new Runnable() {
						public void run() {
							tv.append("Quorum not established!\n");
						}
					});
					throw new Exception();
				}

				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
				if (keyIndex == -1 || valueIndex == -1) {
					Log.e(TAG, "Wrong columns");
					resultCursor.close();
					throw new Exception();
				}

				resultCursor.moveToFirst();

				if (!(resultCursor.isFirst() && resultCursor.isLast())) {
					Log.e(TAG, "Wrong number of rows");
					resultCursor.close();
					throw new Exception();
				}

				final String returnKey = resultCursor.getString(keyIndex);
				final String returnValue = resultCursor.getString(valueIndex);
				handle.post(new Runnable() {
					public void run() {
						tv.append("key:" + returnKey + ",value:" + returnValue + "\n");
					}
				});

				resultCursor.close();
				Thread.sleep(1000);
			}
		} catch (Exception e) {
		}
	}
	
	@Override
	protected void onStop() {
//		This code helps clear the previously stored key value pairs in internal storage		
		/*Uri mUri = buildUri("content",
				"edu.buffalo.cse.cse486586.simpledynamo.provider");
		getContentResolver().delete(mUri, null, null);*/
		shutDown = true;
		if(!socket.isClosed()){
			try {
				Thread.sleep(150);
				socket.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		super.onStop();
	}
}
