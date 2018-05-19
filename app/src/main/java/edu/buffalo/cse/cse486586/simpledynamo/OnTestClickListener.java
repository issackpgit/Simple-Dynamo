package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

public class OnTestClickListener implements View.OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 50;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;

    public OnTestClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }
    // Test
    @Override
    public void onClick(View v) {

        switch(v.getId()){
            case R.id.button1:
                mTextView.setText("");
                new LDump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
                break;

            case R.id.button4:
                mTextView.setText("");
                break;
        }

    }

    private class LDump extends AsyncTask<Void, String, Void>{

        @Override
        protected Void doInBackground(Void... voids) {

            Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);

            int count = 1;
            while (resultCursor.moveToNext()) {
                String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                publishProgress(count+":Key:"+key + "-Val:" + value +"\n");
                count++;
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);
            return;
        }
    }



}

