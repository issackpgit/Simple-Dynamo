package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.Buffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private DBHelper dbHelper;
    String myPort;
    String successor1, successor2;
    String predecessor1, predecessor2;
    int succPort1, succPort2;
    int predPort1, predPort2;
    String key, value;
    private Semaphore lock;
    private HashMap<String, String> starStore = new HashMap();
    boolean flag = false;
    boolean isRecovery = false;

    boolean test = false;

    TreeMap<String, String> ring = new TreeMap<String, String>();
    String[] portList = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;

    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        int rowsDeleted = 0;
        System.out.println("Inside the delete function");

        if (selection.equals("*")) {
            System.out.println("Inside the * delete ");
            rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
            Message msg = new Message("Delete", myPort);
            msg.setKey(selection);
            msg.setSendPort(myPort);
            try {
                sendDelete(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else if (selection.equals("@")) {
            System.out.println("Inside the @ delete ");
            rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
        } else {
            try {
                String cord = findCordinator(genHash(selection));
                String[] sucs = findSuccessors(cord);
                Message msg = new Message("Delete", myPort);
                msg.setKey(selection);
                System.out.println("The key to delete in delete fn :" + selection);
                System.out.println("Cord:" + cord);
                System.out.println("Sucs[0]:" + ring.get(sucs[0]));
                System.out.println("Sucs[1]:" + ring.get(sucs[1]));
                msg.setSendPort(myPort);
                sendDelete1(msg, cord);
                sendDelete1(msg, ring.get(sucs[0]));
                sendDelete1(msg, ring.get(sucs[1]));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return 0;
    }

    private void sendDelete1(Message msg, String port) throws IOException {
        System.out.println("Port is :" + port);
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        out.close();
        writer.close();
        socket.close();
    }

    private void sendDelete(Message msg) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ring.get(successor1)));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        out.close();
        writer.close();
        socket.close();
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {

        if (isRecovery) {
            while (!test) {
                try {
                    System.out.println("Thread sleep in insert");
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        synchronized (this) {
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            String myHash = "";
            String hashKey = "";
            String cordinator = "";

            Log.v("In", "Insert Values");

            String key = (String) values.get("key");
            String value = (String) values.get("value");

            try {
                hashKey = genHash(key);
                myHash = genHash(myPort);
                cordinator = findCordinator(hashKey);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }


            System.out.println("Cordinator :" + cordinator);
            System.out.println("MyPort :" + myPort);
            System.out.println("the key inside the insert 1 :" + key);

            if (Integer.parseInt(myPort) == Integer.parseInt(cordinator) / 2) {
                synchronized (this) {
                    System.out.println("the key inside the insert 2 :" + key);
                    System.out.println("Inside the insert function");
                    db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                    System.out.println("The value with same cordinator :" + key);
                    Message msg = new Message("Replica", myPort);
                    msg.setKey(key);
                    msg.setValue(value);
                    String resp1 = sendClient(msg.toString(), Integer.parseInt(ring.get(successor1)));
                    String resp2 = sendClient(msg.toString(), Integer.parseInt(ring.get(successor2)));
                }

            } else {
                System.out.println("Inside the else of insert");
                System.out.println("the key inside the insert 3 :" + key);
                Message msg = new Message("Insert", myPort);
                msg.setKey(key);
                msg.setValue(value);
                System.out.println("The value is " + value);

                try {
                    String[] sucs = findSuccessors(cordinator);
                    synchronized (this) {
                        String resp = sendClient(msg.toString(), Integer.parseInt(cordinator));
                        String resp1 = sendClient(msg.toString(), Integer.parseInt(ring.get(sucs[0])));
                        String resp2 = sendClient(msg.toString(), Integer.parseInt(ring.get(sucs[1])));
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }
    }

    private String[] findSuccessors(String cordinator) throws NoSuchAlgorithmException {
        String suc1, suc2;

        LinkedList<String> tempList = new LinkedList<String>();

        for (String key : ring.keySet()) {
            tempList.add(key);
        }

        int index = tempList.indexOf(genHash(String.valueOf(Integer.parseInt(cordinator) / 2)));

        if (index == 0) {
            suc1 = tempList.get(1);
            suc2 = tempList.get(2);
        } else if (index == 4) {
            suc1 = tempList.get(0);
            suc2 = tempList.get(1);
        } else if (index == 1) {
            suc1 = tempList.get(2);
            suc2 = tempList.get(3);
        } else if (index == 3) {
            suc1 = tempList.get(4);
            suc2 = tempList.get(0);
        } else {
            suc1 = tempList.get(index + 1);
            suc2 = tempList.get(index + 2);
        }
        return new String[]{suc1, suc2};
    }

    private void sendMessage(Message msg, String suc) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ring.get(suc)));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        out.close();
        writer.close();
        socket.close();
    }

    private String findCordinator(String hashKey) throws NoSuchAlgorithmException {
        int i = 0;
        String[] tempArray = ring.keySet().toArray(new String[5]);
        for (i = 0; i < 4; i++) {
            if (hashKey.compareTo(tempArray[i]) > 0 && hashKey.compareTo(tempArray[i + 1]) < 0)
                break;
        }
        if (i == 4) {
            return ring.get(tempArray[0]);
        } else
            return ring.get(tempArray[i + 1]);
    }

    @Override
    public boolean onCreate() {
        try {
            File file = new File(getContext().getFilesDir().getAbsolutePath());
            BufferedReader reader = new BufferedReader(new FileReader(file + "/" + "Recover"));
            reader.close();
            isRecovery = true;

        } catch (Exception e) {
            System.out.println("No file exception: First run of the file");
            File file = new File(getContext().getFilesDir().getAbsolutePath(), "Recover");
            FileWriter fileWrite = null;
            try {
                fileWrite = new FileWriter(file);
                fileWrite.close();
                isRecovery = false;
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        Log.d(TAG, "main oncreate called");
        dbHelper = new DBHelper(getContext());
        SimpleDynamoActivity sda = new SimpleDynamoActivity();
        lock = new Semaphore(0);

        myPort = sda.getPortNo(getContext());

        for (int i = 0; i < 5; i++) {
            try {
                ring.put(genHash(portList[i]), String.valueOf(Integer.parseInt(portList[i]) * 2));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        printMap(ring);

        int size = ring.size();
        Log.e("Size of ring: ", String.valueOf(size));
        LinkedList<String> tempList = new LinkedList<String>();

        for (String key : ring.keySet()) {
            tempList.add(key);
        }
        int index = 0;

        try {
            index = tempList.indexOf(genHash(myPort));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("Index: ", String.valueOf(index));


        if (index == 0) {
            successor1 = tempList.get(1);
            predecessor1 = tempList.get(size - 1);
            successor2 = tempList.get(2);
            predecessor2 = tempList.get(size - 2);
        } else if (index == 4) {
            successor1 = tempList.get(0);
            predecessor1 = tempList.get(size - 2);
            successor2 = tempList.get(1);
            predecessor2 = tempList.get(size - 3);
        } else if (index == 1) {
            successor1 = tempList.get(2);
            successor2 = tempList.get(3);
            predecessor1 = tempList.get(0);
            predecessor2 = tempList.get(4);
        } else if (index == 3) {
            successor1 = tempList.get(4);
            successor2 = tempList.get(0);
            predecessor1 = tempList.get(2);
            predecessor2 = tempList.get(1);
        } else {
            successor1 = tempList.get(index + 1);
            predecessor1 = tempList.get(index - 1);
            successor2 = tempList.get(index + 2);
            predecessor2 = tempList.get(index - 2);
        }

        succPort1 = Integer.parseInt(ring.get(successor1));
        predPort1 = Integer.parseInt(ring.get(predecessor1));
        succPort2 = Integer.parseInt(ring.get(successor2));
        predPort2 = Integer.parseInt(ring.get(predecessor2));

        System.out.println("Successor1 : " + succPort1);
        System.out.println("Predecessor1 : " + predPort1);
        System.out.println("Successor2 : " + succPort2);
        System.out.println("Predecessor2 : " + predPort2);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        if (isRecovery) {

            System.out.println("ISK There has been a failure for this port :" + myPort);
            synchronized (this) {
                new RecoveryTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, myPort);
            }
            try {
                lock.acquire();
            } catch (InterruptedException e) {
                System.out.println("Not able to Acquire lock");
                e.printStackTrace();
            }
            System.out.println("ISK Recovery done");
        } else {
            System.out.println("ISK Working as normal :" + myPort);
        }

        return false;
    }

    private synchronized String findHead(String myPort) throws NoSuchAlgorithmException {

        LinkedList<String> tempList = new LinkedList<String>();
        for (String key : ring.keySet()) {
            tempList.add(key);
        }
        int index = tempList.indexOf(genHash(myPort));

        index -= 2;
        if (index < 0) {
            index += 5;
        }

        return ring.get(tempList.get(index));

    }

    private class RecoveryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            Message msg = new Message("Recovery", myPort);
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            try {
                System.out.println("Output from successor");
                String outputSucc = sendClient(msg.toString(), Integer.parseInt(ring.get(successor1)));
                System.out.println("Output from predessor");
                String outputPred = sendClient(msg.toString(), Integer.parseInt(ring.get(predecessor1)));
                if (outputSucc.equals("Failure")) {
                    System.out.println("Inside failure 1");
                    outputSucc = sendClient(msg.toString(), Integer.parseInt(ring.get(successor1)));
                }

                if (outputPred.equals("Failure")) {
                    System.out.println("Inside failure 2");
                    outputPred = sendClient(msg.toString(), Integer.parseInt(ring.get(predecessor1)));
                }
                System.out.println("THe output from Recovery : " + outputSucc);
                System.out.println("THe output from Recovery Pred: " + outputPred);
                if (!(outputSucc.length() == 1)) {

                    System.out.println("ISK Inside recovery processing Succ");
                    outputSucc = outputSucc.substring(1, outputSucc.length() - 1);
                    String[] outputs = outputSucc.split("-");
                    for (int i = 0; i < outputs.length; i++) {
                        Log.e("Output in loop ", outputs[i]);
                        String[] outP = outputs[i].split("!");
                        if (findCordinator(genHash(outP[0])).equals(String.valueOf(Integer.parseInt(myPort) * 2))) {
                            ContentValues values = new ContentValues();
                            values.put("key", outP[0]);
                            values.put("value", outP[1]);
                            db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                            System.out.println(outP[0] + " inserted into this node ISK");
                            Message msg1 = new Message("Delete", myPort);
                            msg1.setKey(outP[0]);
                            msg1.setSendPort(myPort);
                        }
                    }
                }

                String head = findHead(myPort);

                if (!(outputPred.length() == 1)) {
                    System.out.println("ISK Inside recovery processing Pred");
                    outputPred = outputPred.substring(1, outputPred.length() - 1);
                    String[] outputs = outputPred.split("-");
                    for (int i = 0; i < outputs.length; i++) {
                        Log.e("Pred Output loop task", outputs[i]);
                        String[] outP = outputs[i].split("!");
                        if (findCordinator(genHash(outP[0])).equals(head) || findCordinator(genHash(outP[0])).equals(ring.get(predecessor1))) {
                            ContentValues values = new ContentValues();
                            values.put("key", outP[0]);
                            values.put("value", outP[1]);
                            db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                            System.out.println(outP[0] + " inserted into this node ISK");
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            test = true;
            lock.release();
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            Log.e(TAG, "Server Task called");
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            while (true) {
                try {
                    Log.e(TAG, "Listening for request");
                    Socket socket = serverSocket.accept();
                    Log.e(TAG, "Socket accepted");
                    InputStream in = socket.getInputStream();
                    OutputStream out = socket.getOutputStream();
                    DataInputStream reader = new DataInputStream(in);
                    DataOutputStream writer = new DataOutputStream(out);

                    String reqMsg = reader.readUTF();

                    System.out.println("isRecovery:" + isRecovery);
                    System.out.println("Test:" + test + ":isrec:" + isRecovery);

                    if (isRecovery) {
                        while (!test) {
                            try {
                                System.out.println("Thread sleep in insert");
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    synchronized (this) {
                        System.out.println("Message: " + reqMsg);
                        String[] parts = reqMsg.split(":");

                        String type = parts[0];

                        if (type.equals("Replica") || type.equals("Insert")) {

                            if (isRecovery)
                                while (!test) ;

                            ContentValues values = new ContentValues();
                            values.put("key", parts[2]);
                            values.put("value", parts[3]);
                            db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                            System.out.println("The value inserted in replica --" + parts[2] + " : " + parts[3]);
                            writer.writeUTF("SUCCESS");
                            socket.close();
                        }
                        if (type.equals("Query")) {

                            if (isRecovery)
                                while (!test) ;

                            String val = "";
                            String selection = parts[2];

                            //Reference code taken from the previous implementation of dht

                            if (selection.equals("*")) {
                                writer.writeUTF("SUCCESS");
                                socket.close();
                                String sendingPort = parts[4];
                                String outStar = "~";
                                System.out.println("Inside the * query of server task");
                                Message starSend = new Message("Query", myPort);
                                starSend.setSendPort(sendingPort);
                                starSend.setKey(selection);
                                System.out.println("Sending port = " + starSend.getSendPort());
                                System.out.println("My Port = " + myPort);
                                if (starSend.getSendPort().equals(myPort)) {
                                    System.out.println("Inside equal condition");
                                    flag = true;
                                } else {
                                    Cursor c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);

                                    if (c == null) {
                                        System.out.println("Cursor is null");
                                        outStar = "~";
                                    }
                                    while (c.moveToNext()) {
                                        key = c.getString(c.getColumnIndex("key"));
                                        value = c.getString(c.getColumnIndex("value"));
                                        outStar += key + "!" + value + "-";
                                    }

                                    Message starReply = new Message("StarReply", myPort);
                                    starReply.setOutStar(outStar);
                                    starReply.setKey(selection);
                                    starReply.setSendPort(sendingPort);
                                    sendOutput(starReply);
                                    sendQuery(starSend);
                                }

                            } else {
                                Cursor c = db.query(dbHelper.TABLE_NAME, null, "key =?", new String[]{selection}, null, null, null);

                                if (c.moveToFirst()) {
                                    System.out.println("C is not null");
                                    val = c.getString(1);
                                }
                                System.out.println("The query value is :" + val);
                                writer.writeUTF(val);
                                socket.close();
                            }
                        } else if (type.equals("StarReply")) {

                            System.out.println("Inside starrrply of server task");
                            System.out.println("reply from " + reqMsg.split(":")[1]);

                            System.out.println("Reply message : " + reqMsg);

                            String output = parts[5];

                            if (!(output.length() == 1)) {

                                output = output.substring(1, output.length() - 1);

                                System.out.println("Output:" + output);

                                String[] outputs = output.split("-");

                                for (int i = 0; i < outputs.length; i++) {
                                    Log.e("Output in loop ", outputs[i]);
                                    String[] outP = outputs[i].split("!");
                                    starStore.put(outP[0], outP[1]);
                                }
                            }
                        } else if (type.equals("Delete")) {
                            String selection = parts[2];
                            String sendingPort = parts[4];
                            String hashKey = genHash(selection);
                            String hashPort = genHash(myPort);
                            int rowsDeleted;

                            if (selection.equals("*")) {
                                Message msg = new Message("Delete", myPort);
                                msg.setSendPort(sendingPort);
                                msg.setKey(selection);
                                if (!(msg.getSendPort().equals(myPort))) {
                                    rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
                                    sendDelete(msg);
                                }
                            } else if (selection.equals("@")) {
                                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
                            } else {
                                System.out.println("Inside the else of delete");
                                System.out.println("The key to delete is :" + selection);
                                if (findCordinator(genHash(selection)).equals(String.valueOf(Integer.parseInt(myPort) * 2))) {
                                    System.out.println("You are doing correct");
                                } else {
                                    System.out.println("you are in a replica");
                                }
                                rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key=?", new String[]{selection});
                                if (rowsDeleted == 1) {
                                    System.out.println(selection + " deleted");
                                } else
                                    System.out.println(selection + " not deleted");
                            }
                        } else if (type.equals("Recovery")) {
                            Cursor c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);

                            String outStar = "~";
                            while (c.moveToNext()) {
                                key = c.getString(c.getColumnIndex("key"));
                                value = c.getString(c.getColumnIndex("value"));
                                outStar += key + "!" + value + "-";
                            }
                            writer.writeUTF(outStar);
                            socket.close();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        private void sendOutput(Message msg) throws IOException {
            String remotePort = msg.getSendPort();
            Log.e("Sending output to ", remotePort);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort) * 2);
            OutputStream out = socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(out);
            writer.writeUTF(msg.toString());
            out.close();
            writer.close();
            socket.close();
        }
    }

    private String findPredecessor(String remotePort) throws NoSuchAlgorithmException {
        LinkedList<String> tempList = new LinkedList<String>();
        for (String key : ring.keySet()) {
            tempList.add(key);
        }
        int index = tempList.indexOf(genHash(String.valueOf(Integer.parseInt(remotePort) / 2)));
        System.out.println("first Index " + index);
        index -= 1;
        if (index < 0)
            index += 5;
        System.out.println("second Index " + index);
        return ring.get(tempList.get(index));
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        if (isRecovery)
            while (!test) {
                try {
                    System.out.println("Thread Sleep");
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        Cursor c = null;
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String hashKey = "";

        if (selection.equals("@")) {
            System.out.println("Inside the query of @");
            System.out.println("Selection is " + selection);
            c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);
            return c;
        } else if (selection.equals("*")) {
            System.out.println("Inside the * of query operation");
            c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);

            Message msg = new Message("Query", myPort);
            msg.setKey(selection);
            msg.setSendPort(myPort);

            try {
                sendQuery(msg);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            while (!flag) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (String key : starStore.keySet()) {
                matrixCursor.addRow(new String[]{key, starStore.get(key)});
            }

            if (c.moveToFirst()) {
                do {
                    key = c.getString(c.getColumnIndex("key"));
                    value = c.getString(c.getColumnIndex("value"));
                    matrixCursor.addRow(new String[]{key, value});
                } while (c.moveToNext());
            }

            return matrixCursor;


        } else {
            System.out.println("Inside single insert of Query");

            System.out.println("Selection ISK == " + selection);

            try {
                hashKey = genHash(selection);
                String readCordinator = findQueryCord(hashKey);
                System.out.println("Query Cordinator :" + readCordinator);
                Message msg = new Message("Query", myPort);
                msg.setKey(selection);
                String resp = sendClient(msg.toString(), Integer.parseInt(readCordinator));
                matrixCursor.addRow(new String[]{selection, resp});
                return matrixCursor;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private void sendQuery(Message msg) throws IOException, NoSuchAlgorithmException {
        String suc = ring.get(successor1);
        System.out.println("Sending --" + msg.toString() + " -- query to " + suc);
        try {
            System.out.println("requesting socket");
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(suc));
            System.out.println("socket connect established");
            OutputStream out = socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(out);
            DataInputStream reader = new DataInputStream((socket.getInputStream()));
            writer.writeUTF(msg.toString());
            writer.flush();
            socket.setSoTimeout(7000);
            String resp = reader.readUTF();
            System.out.println("write completed");
            out.close();
            writer.close();
            socket.close();
        } catch (IOException e) {
            System.out.println("Time out exception in send Query for star");
            String[] succ = findSuccessors(suc);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ring.get(succ[0])));
            System.out.println("socket connect established");
            OutputStream out = socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(out);
            DataInputStream reader = new DataInputStream((socket.getInputStream()));
            writer.writeUTF(msg.toString());
            writer.flush();
            String resp = reader.readUTF();
            System.out.println("write completed");
            out.close();
            writer.close();
            socket.close();
        }
    }

    private String findQueryCord(String hashKey) throws NoSuchAlgorithmException {
        String cord = findCordinator(hashKey);
        LinkedList<String> tempList = new LinkedList<String>();
        for (String key : ring.keySet()) {
            tempList.add(key);
        }
        System.out.println("Cordinator is " + cord);
        int index = tempList.indexOf(genHash(String.valueOf(Integer.parseInt(cord) / 2)));
        System.out.println("Index is " + index);
        index += 2;
        if (index > 4)
            index = index % 5;

        return ring.get(tempList.get(index));
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

    private synchronized String sendClient(String msg, int remotePort) {
        try {
            System.out.println("remote port " + remotePort / 2);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
            DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
            DataInputStream reader = new DataInputStream((socket.getInputStream()));
            System.out.println("msg in sendclient:" + msg);
            writer.writeUTF(msg);
            System.out.println("Write done");
            writer.flush();
            socket.setSoTimeout(10000);
            String resp = reader.readUTF();
            socket.close();
            System.out.println("Value returned in client task from " + remotePort + " : " + resp);
            if (resp.equals("")) {
                throw new IOException();
            }
            return resp;
        } catch (SocketTimeoutException e) {
            Log.d("SocketTimeOut", "Exception for " + Integer.toString(remotePort / 2));
            String type = msg.split(":")[0];
            if (type.equals("Query")) {
                try {
                    String port = findPredecessor(String.valueOf(remotePort));
                    System.out.println("The port inside catch of clienttast :" + port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
                    DataInputStream reader = new DataInputStream((socket.getInputStream()));
                    writer.writeUTF(msg);
                    writer.flush();
                    String resp = reader.readUTF();
                    socket.close();
                    return resp;
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (NoSuchAlgorithmException e1) {
                    e1.printStackTrace();
                }
            }
        } catch (IOException e) {
            String type = msg.split(":")[0];
            if (type.equals("Query")) {
                try {
                    String port = findPredecessor(String.valueOf(remotePort));
                    System.out.println("The port inside catch of clienttast :" + port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
                    DataInputStream reader = new DataInputStream((socket.getInputStream()));
                    writer.writeUTF(msg);
                    writer.flush();
                    String resp = reader.readUTF();
                    socket.close();
                    return resp;
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (NoSuchAlgorithmException e1) {
                    e1.printStackTrace();
                }
            }
            Log.d("IOEXCEPTION", "Timeout on " + Integer.toString(remotePort / 2));
            System.out.println("Recovery is happening in this port");
            System.out.println("Sleeping for 10 secs");

        }
        return "Failure";
    }

    public static <K, V> void printMap(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey()
                    + " Value : " + Integer.parseInt((String) entry.getValue()) / 2);
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
