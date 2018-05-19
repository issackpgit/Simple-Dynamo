package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button1).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));

		findViewById(R.id.button4).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	public String getPortNo(Context context){
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return String.valueOf(Integer.parseInt(portStr));
	}
}
