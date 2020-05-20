package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = SimpleDynamoProvider.class.getName();

    final static Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

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

    static final int SERVER_PORT = 10000;

    String remote_ports[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    TreeMap<String, String> port_nums_map = new TreeMap<String, String>();

    int i = -1;


    String myPort_hashed = "";
    String portStr = "";
    String myPort = "";

    ArrayList<String> keys_map = new ArrayList<String>();


    HashMap<String, HashMap<String, String>> succ_msgs_direct_map = new HashMap<String, HashMap<String, String>>();

    SimpleDateFormat format = new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy");

    ReadWriteLock rwLock = new ReentrantReadWriteLock();

    Lock writeLock = rwLock.writeLock();

    //Method for storing failed key-value pairs
    public void insert_value_in_node_port(HashMap<String, HashMap<String, String>> map, String port, String key, String value) {

        if (map.keySet().contains(port)) {
            map.get(port).put(key, value);
        } else {
            HashMap<String, String> new_map = new HashMap<String, String>();
            new_map.put(key, value);
            map.put(port, new_map);
        }

    }

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    //Method for getting position of next node in the ring
    public int giveNext(int position) {
        if (position < 4) {
            return position + 1;
        } else {
            return 0;
        }
    }

    //Method for getting position of next node of next node in the ring
    public int giveNextNext(int position) {
        if (position < 3) {
            return position + 2;
        } else if (position == 3) {
            return 0;
        } else {
            return 1;
        }
    }

    //Method for finding node's position in the ring
    public int findPortIndex(String port) {
        String port_hashed = "";
        try {
            port_hashed = genHash(port);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.v(TAG, "port_hashed" + port_hashed);


        int j = 0;
        Collections.sort(keys_map);
        for (String str : keys_map) {
            if (port.equals(str)) return j;
            j++;
        }
        return -1;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        writeLock.lock();
        // TODO Auto-generated method stub
        try {
            getContext().deleteFile(selection);
            return 0;
        } finally {
            writeLock.unlock();
        }


    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    //Method for finding the key's partition based on hash value
    public String findLocation(String key) {

        String fileName_hashed = "";

        String result_port = "";

        try {
            fileName_hashed = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (fileName_hashed.compareTo(keys_map.get(4)) > 0 || fileName_hashed.compareTo(keys_map.get(0)) < 0) {
            return port_nums_map.get(keys_map.get(0));
        } else if (fileName_hashed.compareTo(keys_map.get(0)) > 0 && fileName_hashed.compareTo(keys_map.get(1)) < 0) {
            return port_nums_map.get(keys_map.get(1));
        } else if (fileName_hashed.compareTo(keys_map.get(1)) > 0 && fileName_hashed.compareTo(keys_map.get(2)) < 0) {
            return port_nums_map.get(keys_map.get(2));
        } else if (fileName_hashed.compareTo(keys_map.get(2)) > 0 && fileName_hashed.compareTo(keys_map.get(3)) < 0) {
            return port_nums_map.get(keys_map.get(3));
        } else if (fileName_hashed.compareTo(keys_map.get(3)) > 0 && fileName_hashed.compareTo(keys_map.get(4)) < 0) {
            return port_nums_map.get(keys_map.get(4));
        } else {
            return null;
        }

    }


    //Method for sending replicas to next 2 nodes
    public void sendReplicas(String key, String val) {

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", key + " " + val + "-onlyinsert", port_nums_map.get(keys_map.get(giveNext(i))), myPort);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", key + " " + val + "-onlyinsert", port_nums_map.get(keys_map.get(giveNextNext(i))), myPort);

    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        writeLock.lock();
        // TODO Auto-generated method stub
        try {
            String fileName = values.get("key").toString();

            String msgValue = values.get("value").toString();

            Log.v(TAG, "Received for insertion key " + fileName);

            Log.v(TAG, "Received for insertion val " + msgValue);

            String portLocation = findLocation(fileName);

            String[] msg_array1 = msgValue.split("-");
            if (msg_array1.length > 1) {
                //Only for inserting replicated message in file-system
                if (msg_array1[1].equals("onlyinsert")) {
                    Log.d(TAG, "Inserting key for onlyinsert " + fileName + " val " + msg_array1[0]);
                    try {
                        getContext().deleteFile(fileName);
                        FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
                        fos.write((msg_array1[0] + "-" + new Date()).getBytes());
                        fos.close();
                    } catch (FileNotFoundException e) {
                        Log.e(TAG, "File Not Found Exception");
                    } catch (IOException e) {
                        Log.e(TAG, "Input/Output Exception");
                    }

                    return providerUri;
                }

            } else {
                //For messages that are directly inserted
                if (portLocation.equals(myPort)) {
                    sendReplicas(fileName, msgValue);
                    Log.d(TAG, "Inserting key " + fileName + " val " + msgValue);

                    try {
                        getContext().deleteFile(fileName);
                        FileOutputStream fos = getContext().openFileOutput(fileName, MODE_PRIVATE);
                        fos.write((msgValue + "-" + new Date()).getBytes());
                        fos.close();
                    } catch (FileNotFoundException e) {
                        Log.e(TAG, "File Not Found Exception");
                    } catch (IOException e) {
                        Log.e(TAG, "Input/Output Exception");
                    }
                    return providerUri;
                } else {
                    Log.v(TAG, "Doesn't belong in my node ");
                    Log.v(TAG, "Sending msg to crct port " + portLocation);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", fileName + " " + msgValue + "-onlyinsert", portLocation, myPort);
                    int portInd = findPortIndex(portLocation);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", fileName + " " + msgValue + "-onlyinsert", port_nums_map.get(keys_map.get(giveNext(portInd))), myPort);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", fileName + " " + msgValue + "-onlyinsert", port_nums_map.get(keys_map.get(giveNextNext(portInd))), myPort);

                }
            }

            return null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d(TAG, "mPort " + myPort);
        Log.d(TAG, "mPortNum " + portStr);


        try {

            REMOTE_PORT_NUM0 = genHash(REMOTE_PORT_NUM0);
            REMOTE_PORT_NUM1 = genHash(REMOTE_PORT_NUM1);
            REMOTE_PORT_NUM2 = genHash(REMOTE_PORT_NUM2);
            REMOTE_PORT_NUM3 = genHash(REMOTE_PORT_NUM3);
            REMOTE_PORT_NUM4 = genHash(REMOTE_PORT_NUM4);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

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


        Iterator iterator = port_nums_map.keySet().iterator();
        i = 0;
        while (iterator.hasNext()) {
            if (iterator.next().equals(myPort_hashed)) {
                break;
            }
            i++;
        }


        keys_map = new ArrayList<String>(port_nums_map.keySet());
        Collections.sort(keys_map);

        Log.v(TAG, "printing port sin order");
        for (String str : keys_map) {
            Log.v(TAG, port_nums_map.get(str));
        }


        //Recovering lost messages during failure
        Log.v(TAG, "i value " + i + " port " + port_nums_map.get(keys_map.get(i)));
        for (int j = 0; j < 5; j++) {
            if (j != i) {
                Log.v(TAG, "asking " + port_nums_map.get(keys_map.get(j)) + " for copies");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "copy", "copy " + myPort, port_nums_map.get(keys_map.get(j)), myPort);
            }
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

    //Method for sending query
    public String sendQuery(String selection, String port) {
        try {
            String messageFromClient = "";
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            String msgToSend = selection + " query";
            OutputStream outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            printWriter.println(msgToSend);

            InputStream inputStream = socket.getInputStream();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            messageFromClient = bufferedReader.readLine();
            return messageFromClient;
        } catch (IOException e) {
            Log.e(TAG, "Input/Output Exception");
        }
        return null;
    }

    //Method for sending direct query without any further propagation
    public String sendSuccQuery(String selection, String port) {
        try {
            String messageFromClient = "";
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            String msgToSend = selection + " query1";
            OutputStream outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            printWriter.println(msgToSend);

            InputStream inputStream = socket.getInputStream();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            messageFromClient = bufferedReader.readLine();
            return messageFromClient;
        } catch (IOException e) {
            Log.e(TAG, "Input/Output Exception");
        }
        return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        try {
            Log.d(TAG, "selection " + selection);
            String[] selection_splits = selection.split("\\s+");
            if (selection_splits.length == 1) {
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
                        cursor.addRow(new String[]{str, msg.split("-")[0]});
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
                        cursor.addRow(new String[]{str, msg.split("-")[0]});
                        Log.v("msg", msg);
                    }

                    for (String rm_port : remote_ports) {
                        if (!rm_port.equals(myPort)) {
                            try {
                                Log.d(TAG, "sending query @ to port " + rm_port);
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
                                if (messageFromClient != null) {
                                    if (!messageFromClient.equals("")) {
                                        if (!messageFromClient.equals(" ")) {
                                            //MatrixCursor cursor1 = new MatrixCursor(new String[]{"key", "value"});
                                            String[] pairs = messageFromClient.split("\\|");
                                            for (String st : pairs) {
                                                Log.d(TAG, "pair " + st);
                                            }
                                            for (String pair : pairs) {
                                                pair.trim();
                                                String[] splits = pair.split("\\s+");
//												for (String st : splits) {
//													Log.d(TAG, "split " + st);
//												}
                                                cursor.addRow(new String[]{splits[0], splits[1].split("-")[0]});
                                            }
                                        }
                                    }
                                }

                            } catch (IOException ex) {
                                ex.printStackTrace();

                            } catch (NullPointerException e) {
                                Log.d(TAG, e.getMessage());
                            }
                        }
                    }

                    return cursor;
                } else {

                    String msg = "";
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    String currValue = null;
                    Date currDate = null;
                    try {
                        Log.d(TAG, "selection inside cond" + selection);

                        FileInputStream fis = getContext().openFileInput(selection);
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                        msg = bufferedReader.readLine();
                        Log.d(TAG, "message " + msg);
                        fis.close();
                        if (msg != null) {
                            currValue = msg.split("-")[0];
                            currDate = format.parse(msg.split("-")[1]);
                        }

                    } catch (FileNotFoundException e) {
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    String portLocation = findLocation(selection);

                    Log.v(TAG, "selection belongs to " + portLocation);
                    String messageFromClient = "";
                    if (!portLocation.equals(myPort)) {
                        messageFromClient = sendSuccQuery(selection, portLocation);
                    }

                    if (messageFromClient != null) {
                        if (!messageFromClient.equals("")) {
                            String[] splits = messageFromClient.split("_");
                            Log.v(TAG, "splits[1] " + splits[1]);
                            String[] sec_splits = splits[1].split("-");
                            Log.v(TAG, "sec_splits[0] " + sec_splits[0]);
                            Log.v(TAG, "sec_splits[1] " + sec_splits[1]);
                            if (currValue != null) {
                                if (currDate.compareTo(format.parse(sec_splits[1])) < 0) {
                                    currValue = sec_splits[0];
                                    currDate = format.parse(sec_splits[1]);
                                }
                            } else {
                                currValue = sec_splits[0];
                                currDate = format.parse(sec_splits[1]);
                            }


                        }
                    }


                    int ind = findPortIndex(portLocation);
                    Log.v(TAG, "index of successor " + ind);
                    Log.v(TAG, "selection successor to " + port_nums_map.get(keys_map.get(giveNext(ind))));
                    String messageFromClient1 = null;

                    if (!port_nums_map.get(keys_map.get(giveNext(ind))).equals(myPort)) {
                        Log.v(TAG, "selection successor to " + port_nums_map.get(keys_map.get(giveNext(ind))));
                        messageFromClient1 = sendSuccQuery(selection, port_nums_map.get(keys_map.get(giveNext(ind))));
                    }

                    Log.d(TAG, "msg received from client for query " + messageFromClient1);
                    if (messageFromClient1 != null) {
                        if (!messageFromClient1.equals("")) {
                            String[] splits = messageFromClient1.split("_");
                            Log.v(TAG, "splits[1] " + splits[1]);
                            String[] sec_splits = splits[1].split("-");
                            Log.v(TAG, "sec_splits[0] " + sec_splits[0]);
                            Log.v(TAG, "sec_splits[1] " + sec_splits[1]);
                            if (currValue != null) {
                                if (currDate.compareTo(format.parse(sec_splits[1])) < 0) {
                                    currValue = sec_splits[0];
                                    currDate = format.parse(sec_splits[1]);
                                }
                            } else {
                                currValue = sec_splits[0];
                                currDate = format.parse(sec_splits[1]);
                            }
                        }
                    }


                    String messageFromClient2 = null;
                    if (!port_nums_map.get(keys_map.get(giveNextNext(ind))).equals(myPort)) {
                        Log.v(TAG, "selection successor to " + port_nums_map.get(keys_map.get(giveNextNext(ind))));
                        messageFromClient2 = sendSuccQuery(selection, port_nums_map.get(keys_map.get(giveNextNext(ind))));
                    }

                    Log.d(TAG, "msg received from client for query " + messageFromClient2);
                    if (messageFromClient2 != null) {
                        if (!messageFromClient2.equals("")) {
                            String[] splits = messageFromClient2.split("_");
                            Log.v(TAG, "splits[1] " + splits[1]);
                            String[] sec_splits = splits[1].split("-");
                            Log.v(TAG, "sec_splits[0] " + sec_splits[0]);
                            Log.v(TAG, "sec_splits[1] " + sec_splits[1]);
                            if (currValue != null) {
                                if (currDate.compareTo(format.parse(sec_splits[1])) < 0) {
                                    currValue = sec_splits[0];
                                    currDate = format.parse(sec_splits[1]);
                                }
                            } else {
                                currValue = sec_splits[0];
                                currDate = format.parse(sec_splits[1]);
                            }
                        }
                    }

                    cursor.addRow(new String[]{selection, currValue});
                    Log.v("msg", selection);
                    Log.v("query", currValue);
                    return cursor;

                }
            } else {
                //Response for direct query
                if (selection_splits[1].equals("query1")) {
                    String msg = "";
                    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                    try {
                        //Log.d(TAG, "selection "+selection);

                        FileInputStream fis = getContext().openFileInput(selection_splits[0]);
                        //FileInputStream fiis = getContext().ge
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                        msg = bufferedReader.readLine();
                        Log.d(TAG, "message " + msg);
                        fis.close();
                        if (msg != null && msg != "") {
                            cursor.addRow(new String[]{selection_splits[0], msg});
                            Log.v("msg", msg);
                            Log.v("query", selection_splits[0]);
                            return cursor;
                        } else {
                            return null;
                        }

                    } catch (FileNotFoundException e) {
                        Log.e(TAG, "file not found");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
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

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected synchronized Void doInBackground(String... msgs) {
            try {
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                /*
                 *The Socket API used is learnt from  Oracle tutorials on Socket Communications(suggested by professor)
                 *URL: https://www.oracle.com/technetwork/java/socket-140484.html
                 */
                //Writing the String msgToSend to output stream of data
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
                        Log.v(TAG, "Inserting for recovery");
                        insert_value_in_node_port(succ_msgs_direct_map, remote_port, msgToSend.split("\\s+")[0], msgToSend.split("\\s+")[1]);
//					}
                    }
                } else if (msgs[0].equals("copy")) {
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
                    if (messageFromClient != null) {
                        if (!messageFromClient.equals("")) {
                            String[] pairs = messageFromClient.split("\\|");
                            for (String st : pairs) {
                                Log.d(TAG, "copy pair " + st);
                            }
                            for (String pair : pairs) {
                                pair.trim();
                                String[] splits = pair.split(("_"));

                                for (String st : splits) {
                                    Log.d(TAG, "copy split " + st);
                                }
                                Log.d(TAG, "key copy received" + splits[0]);
                                Log.d(TAG, "val copy received" + splits[1]);

                                ContentValues keyValueToInsert = new ContentValues();

                                keyValueToInsert.put("key", splits[0]);
                                keyValueToInsert.put("value", splits[1] + "-onlyinsert");

                                Uri newUri = getContext().getContentResolver().insert(SimpleDynamoProvider.providerUri, keyValueToInsert);

                            }
                        }
                    }
                } else {
                    String remote_port = msgs[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    String msgToSend = msgs[0];
                    Log.v(TAG, "Inside client, sending msg " + msgToSend + " to " + remote_port);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.println(msgToSend);
                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String messageFromClient = bufferedReader.readLine();
                    Log.v(TAG, "Response from server " + messageFromClient);
                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + e);
            } catch (NullPointerException e) {
            }
            return null;

        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        int msg_count = 0;

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
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
                    if (messageFromClient != null) {
                        if (!messageFromClient.equals("")) {
                            String[] msgs_received = messageFromClient.split("\\s+");
                            String key = msgs_received[0];

                            String value = msgs_received[1];
                            Log.d(TAG, "key server " + key + " val server" + value);

                            if (value.equals("query")) {
                                try {
                                    Thread.sleep(300);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                Log.d(TAG, "received query from client" + key);
                                Cursor cursor = getContext().getContentResolver().query(providerUri, null, key, null, null);
                                StringBuilder sb = new StringBuilder();
                                if (cursor != null && cursor.getCount() > 0) {
                                    while (cursor.moveToNext()) {
                                        sb.append(cursor.getString(0));
                                        sb.append(" ");
                                        sb.append(cursor.getString(1));
                                        sb.append("|");
                                    }
                                }
                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                //System.out.println("Proposing "+initSeqClient+"."+mPort);
                                printWriter.println(sb.toString());
//								readLock.unlock();
                                Log.v(TAG, "printing ths sb from cursor" + sb.toString());
                            } else if (value.equals("query1")) {
                                Log.d(TAG, "received query from client" + key);
                                Cursor cursor = getContext().getContentResolver().query(providerUri, null, key + " " + value, null, null);
                                StringBuilder sb = new StringBuilder();
                                if (cursor != null && cursor.getCount() > 0) {
                                    while (cursor.moveToNext()) {
                                        sb.append(cursor.getString(0));
                                        sb.append("_");
                                        sb.append(cursor.getString(1));
                                        sb.append("|");
                                    }
                                }
                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                //System.out.println("Proposing "+initSeqClient+"."+mPort);
                                printWriter.println(sb.toString());
                                Log.v(TAG, "printing ths sb from cursor" + sb.toString());
                            } else if (key.equals("copy")) {
                                Log.d(TAG, "value for copy is " + value);

                                StringBuilder sb = new StringBuilder();
                                if (succ_msgs_direct_map.keySet().contains(value)) {
                                    for (String str : succ_msgs_direct_map.get(value).keySet()) {
                                        sb.append(str);
                                        sb.append("_");
                                        Log.v(TAG, "appending " + succ_msgs_direct_map.get(value).get(str));
                                        sb.append(succ_msgs_direct_map.get(value).get(str));
                                        sb.append("|");
                                    }
                                    succ_msgs_direct_map.remove(value);
                                }

                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                printWriter.println(sb.toString());
                                Log.v(TAG, "sending ths copies to " + value + " " + sb.toString());
                            } else {
                                publishProgress(messageFromClient);
                                OutputStream outputStream = socket.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(outputStream, true);
                                printWriter.println("success");
                            }
                        }
                    }
                }


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

            Uri newUri = getContext().getContentResolver().insert(SimpleDynamoProvider.providerUri, keyValueToInsert);
            return;
        }
    }
}
