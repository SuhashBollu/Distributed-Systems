package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.content.ContextWrapper;
import android.widget.TextView;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getName();
    //final static Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    String REMOTE_PORT_NUM0 = "5554";
    String REMOTE_PORT_NUM1 = "5556";
    String REMOTE_PORT_NUM2 = "5558";
    String REMOTE_PORT_NUM3 = "5560";
    String REMOTE_PORT_NUM4 = "5562";


    String myPort_hashed = "";
    String predecessor_hashed = "";
    String successor_hashed = "";
    String predecessor_port = "";
    String successor_port = "";
    String primary_port = "";
    String portStr = "";
    String myPort = "";
    ArrayList<String> msgs_received = new ArrayList<String>();


    static final int SERVER_PORT = 10000;


    String remote_ports[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    TreeMap<String, String> port_nums_map = new TreeMap<String, String>();

    TreeMap<String, String> active_port_nums_map = new TreeMap<String, String>();

    TreeMap<String, String> active_port_nums_map_hashed = new TreeMap<String, String>();


    static Uri providerUri = null;


    private static Uri buildUri(String scheme, String authority, String path) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        Log.d(TAG, "selection " + selection);
        getContext().deleteFile(selection);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String fileName = values.get("key").toString();

        String msgValue = values.get("value").toString();

        Log.v(TAG, "Received for insertion key " + fileName);

        Log.v(TAG, "Received for insertion val " + msgValue);


        String[] value_array = msgValue.split("\\s+");

        if (value_array.length > 1) {

            Log.d(TAG, "Inserting key " + fileName + " val " + msgValue);
            try {
                FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
                fos.write(msgValue.getBytes());
                fos.close();
            } catch (FileNotFoundException e) {
                Log.e(TAG, "File Not Found Exception");
            } catch (IOException e) {
                Log.e(TAG, "Input/Output Exception");
            }

            return providerUri;

        }

        String fileName_hashed = "";


        try {
            fileName_hashed = genHash(fileName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "fileName_hashed.compareTo(myPort_hashed) " + Integer.toString(fileName_hashed.compareTo(myPort_hashed)));
        Log.d(TAG, "fileName_hashed.compareTo(predecessor_hashed) " + Integer.toString(fileName_hashed.compareTo(predecessor_hashed)));
//        if(!msgs_received.contains(fileName)){
//            msgs_received.add(fileName);
        if (myPort.equals(primary_port) && (fileName_hashed.compareTo(myPort_hashed) > 0 && fileName_hashed.compareTo(predecessor_hashed) > 0 || fileName_hashed.compareTo(myPort_hashed) < 0 && fileName_hashed.compareTo(predecessor_hashed) < 0)) {
            Log.d(TAG, "Inserting key " + fileName + " val " + msgValue);
            try {
                FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
                fos.write(msgValue.getBytes());
                fos.close();
            } catch (FileNotFoundException e) {
                Log.e(TAG, "File Not Found Exception");
            } catch (IOException e) {
                Log.e(TAG, "Input/Output Exception");
            }

            return providerUri;
        }

        if (fileName_hashed.compareTo(myPort_hashed) <= 0 && fileName_hashed.compareTo(predecessor_hashed) > 0) {

            Log.d(TAG, "Inserting key " + fileName + " val " + msgValue);
            try {
                FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
                fos.write(msgValue.getBytes());
                fos.close();
            } catch (FileNotFoundException e) {
                Log.e(TAG, "File Not Found Exception");
            } catch (IOException e) {
                Log.e(TAG, "Input/Output Exception");
            }

            return providerUri;
        } else {
            if(successor_port!=null){
                if(!successor_port.equals("")){
            Log.v(TAG, "Sending msg to successor " + successor_port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert",fileName + " " + msgValue, successor_port, myPort);}else{
                Log.d(TAG, "successor port null");
                ContentValues keyValueToInsert = new ContentValues();

                keyValueToInsert.put("key", fileName);
                keyValueToInsert.put("value", msgValue + " failed");

                Uri newUri = getContext().getContentResolver().insert(SimpleDhtProvider.providerUri, keyValueToInsert);
            }}
            return null;
        }
//        }else{
//            Log.d(TAG, "Inserting key esacping contains" + fileName + " val " + msgValue);
//            try {
//                FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
//                fos.write(msgValue.getBytes());
//                fos.close();
//            } catch (FileNotFoundException e) {
//                Log.e(TAG, "File Not Found Exception");
//            } catch (IOException e) {
//                Log.e(TAG, "Input/Output Exception");
//            }
//
//            return providerUri;
//        }

//        Log.d(TAG, "Inserting key "+fileName+" val "+msgValue);
//        try {
//            FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
//            fos.write(msgValue.getBytes());
//            fos.close();
//        } catch (FileNotFoundException e) {
//            Log.e(TAG, "File Not Found Exception");
//        } catch (IOException e) {
//            Log.e(TAG, "Input/Output Exception");
//        }
//
//        return providerUri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d(TAG, "mPort " + myPort);
        Log.d(TAG, "mPortNum " + portStr);

        providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider", myPort);

        if(portStr.equals("5554")){
            active_port_nums_map.put(portStr, myPort);
        }


        String hashed_myport_num = "";
        try {
            hashed_myport_num = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(portStr.equals("5554")){
            active_port_nums_map_hashed.put(hashed_myport_num, myPort);
        }

        Log.v(TAG, "REMOTE_PORT_NUM0" + REMOTE_PORT_NUM0);
        Log.v(TAG, "REMOTE_PORT_NUM1" + REMOTE_PORT_NUM1);
        Log.v(TAG, "REMOTE_PORT_NUM2" + REMOTE_PORT_NUM2);
        Log.v(TAG, "REMOTE_PORT_NUM3" + REMOTE_PORT_NUM3);
        Log.v(TAG, "REMOTE_PORT_NUM4" + REMOTE_PORT_NUM4);


        port_nums_map.put(REMOTE_PORT_NUM0, REMOTE_PORT0);
        port_nums_map.put(REMOTE_PORT_NUM1, REMOTE_PORT1);
        port_nums_map.put(REMOTE_PORT_NUM2, REMOTE_PORT2);
        port_nums_map.put(REMOTE_PORT_NUM3, REMOTE_PORT3);
        port_nums_map.put(REMOTE_PORT_NUM4, REMOTE_PORT4);


        try {
            myPort_hashed = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.v(TAG, "myPort_hashed" + myPort_hashed);


        //Finding available nodes for join
        if(!portStr.equals("5554")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "request","request "+portStr, "11108", myPort);
        }



        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            Log.d(TAG, "Creating server socket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket" + e);

        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.d(TAG, "selection " + selection);
        if (selection.equals("@")) {
            Log.v(TAG, "inside @");
            String[] list_of_files = getContext().fileList();
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            for (String str : list_of_files) {
                String msg = "";
                try {
                    Log.d(TAG, "selection " + str);
                    FileInputStream fis = getContext().openFileInput(str);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    msg = bufferedReader.readLine();
                    Log.d(TAG, "message " + msg);
                    fis.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File Not Found Exception");
                } catch (IOException e) {
                    Log.e(TAG, "Input/Output Exception");
                }
                cursor.addRow(new String[]{str, msg});
                Log.v("msg", msg);
            }
            return cursor;

        } else if (selection.equals("*")) {
            Log.v(TAG, "inside *");
            String[] list_of_files = getContext().fileList();
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            for (String str : list_of_files) {
                String msg = "";
                try {
                    Log.d(TAG, "selection " + str);
                    FileInputStream fis = getContext().openFileInput(str);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    msg = bufferedReader.readLine();
                    Log.d(TAG, "message " + msg);
                    fis.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File Not Found Exception");
                } catch (IOException e) {
                    Log.e(TAG, "Input/Output Exception");
                }
                cursor.addRow(new String[]{str, msg});
                Log.v("msg", msg);
            }

            for(String rm_port:remote_ports){
                if(!rm_port.equals(myPort)){
                    try {
                        Log.d(TAG, "sending query @ to port "+rm_port);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(rm_port));
                        String msgToSend = "@ query";
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        printWriter.println(msgToSend);

                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        //socket.close();
                        String messageFromClient = bufferedReader.readLine();

                        Log.d(TAG, "msg received from client for query " + messageFromClient);
                        if(messageFromClient!=null){
                            if(!messageFromClient.equals("")){
                                if(!messageFromClient.equals(" ")){
                                    //MatrixCursor cursor1 = new MatrixCursor(new String[]{"key", "value"});
                                    String[] pairs = messageFromClient.split("\\|");
                                    for(String st:pairs){
                                        Log.d(TAG, "pair "+st);
                                    }
                                    for(String pair: pairs){
                                        pair.trim();
                                        String[] splits = pair.split("\\s+");
                                        for(String st: splits){
                                            Log.d(TAG, "split "+st);
                                        }
                                        cursor.addRow(new String[]{splits[0], splits[1]});
                                    }}}}

                    } catch (IOException ex) {
                        ex.printStackTrace();

                    }catch (NullPointerException e){
                        Log.d(TAG, e.getMessage());
                    }
                }}

            return cursor;
        } else {
            String msg = "";

            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            try {

                FileInputStream fis = getContext().openFileInput(selection);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                msg = bufferedReader.readLine();
                Log.d(TAG, "message " + msg);
                fis.close();

                cursor.addRow(new String[]{selection, msg});
                Log.v("msg", msg);
                Log.v("query", selection);

            } catch (FileNotFoundException e) {
                Log.e(TAG, "File Not Found Exception");
                try {
                    Log.d(TAG, "sending query to successor"+successor_port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor_port));
                    String msgToSend = selection+" query";
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.println(msgToSend);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String messageFromClient = bufferedReader.readLine();

                    Log.d(TAG, "msg received from client for query " + messageFromClient);

                    MatrixCursor cursor1 = new MatrixCursor(new String[]{"key", "value"});
                    String[] pairs = messageFromClient.split("\\|");
                    for(String st:pairs){
                        Log.d(TAG, "pair "+st);
                    }
                    for(String pair: pairs){
                        pair.trim();
                        String[] splits = pair.split("\\s+");
                        for(String st: splits){
                            Log.d(TAG, "split "+st);
                        }
                        cursor1.addRow(new String[]{splits[0], splits[1]});
                    }
                    return cursor1;

                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } catch (IOException e) {
                Log.e(TAG, "Input/Output Exception");
            }

            return cursor;




        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                /*
                 *The Socket API used is learnt from  Oracle tutorials on Socket Communications(suggested by professor)
                 *URL: https://www.oracle.com/technetwork/java/socket-140484.html
                 */
                //Writing the String msgToSend to output stream of data

                //String[] msgToSend_array = msgToSend.split("\\s+");

                if (msgs[0].equals("insert")) {
                    String remote_port = msgs[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    String msgToSend = msgs[1];
                    Log.v(TAG, "Inside client, sending msg for insert to " + remote_port);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.println(msgToSend);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String messageFromClient = bufferedReader.readLine();

                    Log.d(TAG, "msg received from string " + messageFromClient);
                    if (messageFromClient == null) {
                        Log.d(TAG, "msg received for split " + msgs[1]);
                        String[] strReceived = msgs[1].split("\\s+");

                        Log.d(TAG, "key received" + strReceived[0]);
                        Log.d(TAG, "val received" + strReceived[1]);


                        ContentValues keyValueToInsert = new ContentValues();

                        keyValueToInsert.put("key", strReceived[0]);
                        keyValueToInsert.put("value", strReceived[1] + " failed");

                        Uri newUri = getContext().getContentResolver().insert(SimpleDhtProvider.providerUri, keyValueToInsert);

                    }
                }else if(msgs[0].equals("request")){
                    String remote_port = msgs[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    String msgToSend = msgs[1];
                    Log.v(TAG, "Inside client, sending msg for request to  " + remote_port);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.println(msgToSend);
                }
                else {
                    String remote_port = msgs[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    String msgToSend = msgs[0];
                    Log.v(TAG, "Inside client, sending msg to " + remote_port);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.println(msgToSend);
                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (NullPointerException e) {
            }

            return null;

        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        int msg_count = 0;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            /*
             *The Socket API used is learnt from  Oracle tutorials(suggested by professor) on socket programming
             *URL: https://www.oracle.com/technetwork/java/socket-140484.html
             */

            try {
                //Server listening for incoming client connections and sending the message from client to UI Thread
                //While loop to keep the server listening for client requests
                while (true) {
                    //Listens for client requests
                    Socket socket = serverSocket.accept();
                    //Gets byte stream from client
                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    //Reads and stores the data in a string
                    String messageFromClient = bufferedReader.readLine();

                    Log.v(TAG, "Received message from client " + messageFromClient);
                    if(messageFromClient!=null&&!messageFromClient.equals("")){

                    String[] msgs_received = messageFromClient.split("\\s+");


                    String key = msgs_received[0];

                    String value = msgs_received[1];
                    Log.d(TAG, "key server "+key+" val server"+value);
                    if(portStr.equals("5554")){
                        if(key.equals("request")){
                            Log.d(TAG, "received node request");
                            active_port_nums_map.put(value, port_nums_map.get(value));



                            String hashed_num = "";
                            try {
                                hashed_num = genHash(value);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }

                            active_port_nums_map_hashed.put(hashed_num, port_nums_map.get(value));

                            Log.d(TAG, "printing values in map");
                            for(String st: active_port_nums_map_hashed.keySet()){
                                Log.d(TAG, st+" "+active_port_nums_map_hashed.get(st));
                            }

                            StringBuilder sb = new StringBuilder();
                            for(String st: active_port_nums_map.keySet()){
                                sb.append(st);
                                sb.append(" ");
                            }
                            for(String st : active_port_nums_map.values()){

                                if(!st.equals("11108")){
                                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(st));
                                    String msgToSend = "nodes "+sb.toString();
                                    OutputStream outputStream = socket1.getOutputStream();
                                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                                    printWriter.println(msgToSend);
                                }
                            }


                            int i = 0;
                            Iterator iterator = active_port_nums_map_hashed.keySet().iterator();
                            while (iterator.hasNext()) {
                                if (iterator.next().equals(myPort_hashed)) {
                                    break;
                                }
                                i++;
                            }

                            primary_port = active_port_nums_map_hashed.get(active_port_nums_map_hashed.firstKey());

                            Log.v(TAG, "primary_port" + primary_port);

                            Log.d(TAG, "my loc "+i);

                            ArrayList<String> keys_map = new ArrayList<String>(active_port_nums_map_hashed.keySet());
                            if (i > 0) {
                                predecessor_hashed = keys_map.get(i - 1);
                            } else {
                                predecessor_hashed = keys_map.get(keys_map.size() - 1);
                            }
                            if (i < keys_map.size() - 1) {
                                successor_hashed = keys_map.get(i + 1);
                            } else {
                                successor_hashed = keys_map.get(0);
                            }

                            predecessor_port = active_port_nums_map_hashed.get(predecessor_hashed);
                            successor_port = active_port_nums_map_hashed.get(successor_hashed);

                            Log.v(TAG, "predecessor_port" + predecessor_port);
                            Log.v(TAG, "successor_port" + successor_port);
                        }
                    }


                    if(!portStr.equals("5554")){

                        if(key.equals("nodes")){
                            Log.d(TAG, "received nodes list");
                            active_port_nums_map.clear();
                            for(int i =0;i<msgs_received.length;i++){

                                if(i!=0){
                                    String curr_hashed= "";
                                    try {
                                        curr_hashed = genHash(msgs_received[i]);
                                    } catch (NoSuchAlgorithmException e) {
                                        e.printStackTrace();
                                    }
                                    active_port_nums_map_hashed.put(curr_hashed, port_nums_map.get(msgs_received[i]));
                                }
                            }


                            Log.d(TAG, "printing values in map");
                            for(String st: active_port_nums_map_hashed.keySet()){
                                Log.d(TAG, st+" "+active_port_nums_map_hashed.get(st));
                            }


                            int i = 0;
                            Iterator iterator = active_port_nums_map_hashed.keySet().iterator();
                            while (iterator.hasNext()) {
                                if (iterator.next().equals(myPort_hashed)) {
                                    break;
                                }
                                i++;
                            }



                            primary_port = active_port_nums_map_hashed.get(active_port_nums_map_hashed.firstKey());

                            Log.v(TAG, "primary_port" + primary_port);

                            Log.d(TAG, "my loc "+i);

                            ArrayList<String> keys_map = new ArrayList<String>(active_port_nums_map_hashed.keySet());
                            if (i > 0) {
                                predecessor_hashed = keys_map.get(i - 1);
                            } else {
                                predecessor_hashed = keys_map.get(keys_map.size() - 1);
                            }
                            if (i < keys_map.size() - 1) {
                                successor_hashed = keys_map.get(i + 1);
                            } else {
                                successor_hashed = keys_map.get(0);
                            }

                            predecessor_port = active_port_nums_map_hashed.get(predecessor_hashed);
                            successor_port = active_port_nums_map_hashed.get(successor_hashed);

                            Log.v(TAG, "predecessor_port" + predecessor_port);
                            Log.v(TAG, "successor_port" + successor_port);
                        }
                    }



                    if(value.equals("query")){
                        Log.d(TAG, "recievd query from client"+key);
                        Cursor cursor = getContext().getContentResolver().query(providerUri, null, key, null, null);
                        StringBuilder sb = new StringBuilder();
                        while(cursor.moveToNext()){
                            sb.append(cursor.getString(0));
                            sb.append(" ");
                            sb.append(cursor.getString(1));
                            sb.append("|");
                        }
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        printWriter.println(sb.toString());
                        Log.v(TAG, "printing ths sb from cursor"+sb.toString());
                    }else {
                        if (!key.equals("nodes") && !key.equals("request")) {
                            String key_hashed = "";
                            try {
                                key_hashed = genHash(key);
                            } catch (Exception e) {

                            }

                            if (key_hashed.compareTo(myPort_hashed) < 0) {
                                publishProgress(messageFromClient);
                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                printWriter.println("success");
                            } else {
                                publishProgress(messageFromClient);
                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                printWriter.println("success");
                            }
                        }


                    }

                }}
            } catch (IOException e) {
                Log.e(TAG, "Cannot read input stream from client");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String[] strReceived = strings[0].split("\\s+");

            Log.d(TAG, "key received" + strReceived[0]);
            Log.d(TAG, "val received" + strReceived[1]);

            ContentValues keyValueToInsert = new ContentValues();

            keyValueToInsert.put("key", strReceived[0]);
            keyValueToInsert.put("value", strReceived[1]);
            msg_count++;

            Uri newUri = getContext().getContentResolver().insert(SimpleDhtProvider.providerUri, keyValueToInsert);
            return;
        }
    }
}
