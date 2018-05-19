package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

// Reference : https://developer.android.com
final class DBHelper extends SQLiteOpenHelper {

    private static final int DB_VERSION = 4;
    private static final String DB_NAME = "data.db";
    static final String TABLE_NAME = "datatable";
    static final String KEY_COL = "key";
    static final String VALUE_COL = "value";

    DBHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        String sqlline ="CREATE TABLE " + TABLE_NAME + " (" + KEY_COL + " VARCHAR(255) PRIMARY KEY," + VALUE_COL + " VARCHAR(255) " + ");";
        Log.e("Test", sqlline);
        sqLiteDatabase.execSQL(sqlline);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(sqLiteDatabase);
    }

}