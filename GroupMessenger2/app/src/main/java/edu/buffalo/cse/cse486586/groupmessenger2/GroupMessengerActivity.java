package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static String REMOVE_PORT = "";
    static final int SERVER_PORT = 10000;


    String remote_ports[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    //HashMap<String, Integer> sequence = new HashMap<String, Integer>();
    String mPort;
    StringBuilder sb = new StringBuilder();
    int localMsgId;

    ArrayList<Double> sequenceList = new ArrayList<Double>();
    //ClientOtherTask clientOtherTask;
    ArrayList<String> rem_ports = new ArrayList<String>();





    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.d("portStr "+portStr, TAG);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d("My port no. "+myPort, TAG);
        System.out.println("My port no. "+myPort);
        //StringBuilder sbd;

         mPort = myPort;
         sb.append(myPort);
         sb.append(0);
        localMsgId = Integer.parseInt(sb.toString());

        //System.out.println("Size "+rem_ports.size());

        for (int i = 0; i < remote_ports.length; i++) {
            rem_ports.add(remote_ports[i]);
        }

        System.out.println("Size "+rem_ports.size());

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        String msg = editText.getText().toString() + "\n";
        editText.setText(""); // This is one way to reset the input box.*/
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                /*
                 * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                 * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                 * the difference, please take a look at
                 * http://developer.android.com/reference/android/os/AsyncTask.html
                 */
                //clientTask = new ClientTask();
                //new ClientTask().onProgressUpdat(msg, myPort, "0","msg");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort, "0");
                //new ClientOtherTask.ClientTask()


            }
        });


        /*try {
            Log.d(TAG, "Creating client other task");
            clientOtherTask = new ClientOtherTask();
            clientOtherTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        } catch (Exception e) {
            Log.d("Can'tcreateclientsocket", TAG);
        }*/

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            Log.d(TAG,"Creating server socket");
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
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        int initSeqClient = 0;
        int msg_count = 0;
        //PriorityQueue<Message> priorityQueue = new PriorityQueue<Message>();
        ArrayList<Message> aList = new ArrayList<Message>();
        String currentPort = "";

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


            //try {
                //Server listening for incoming client connections and sending the message from client to UI Thread
                //While loop to keep the server listening for client requests
                while (true) {


                    while (!aList.isEmpty()) {
                        System.out.println("PPPQ--------------------------");
                        ArrayList<Message> msgToRemove = new ArrayList<Message>();
                        for(Message m: aList){
                            System.out.println("PPPQ"+m.getMsgId()+" "+m.getSeqNum()+" "+m.getMsg()+" "+m.getDeliverable());
                            if(REMOVE_PORT!=""&&Integer.toString(m.getMsgId()).substring(0,5).equals(REMOVE_PORT)&&m.getDeliverable()==false){
                                System.out.println("Removing message "+m.getMsgId()+" "+m.getMsg());
                                msgToRemove.add(m);
                                //aList.remove(m);
                            }
                        }
                        aList.removeAll(msgToRemove);
                        msgToRemove.clear();
                        Log.i("Inside ", TAG);
                        Collections.sort(aList);
                        if (!aList.isEmpty()&&aList.get(0).getDeliverable()) {
                            System.out.println("Printing");
                            //Message m = priorityQueue.poll();
                            Message m = aList.get(0);
                            aList.remove(aList.get(0));
                            //Collections.sort(aList);
                            //Log.d("PPPQ"+m.getMsgId()+" "+m.getSeqNum()+" "+m.getMsg()+" "+m.getDeliverable(), TAG);
                            publishProgress(m.getMsg(), Integer.toString(msg_count));
                            ContentValues keyValueToInsert = new ContentValues();
                            //System.out.println("msg_count "+msg_count);
                            keyValueToInsert.put("key", Integer.toString(msg_count));
                            keyValueToInsert.put("value", m.getMsg());
                            //Log.d(TAG, "key "+msg_count+" Value "+m.getMsg());
                            Uri newUri = getContentResolver().insert(GroupMessengerProvider.providerUri, keyValueToInsert);
                            msg_count = msg_count + 1;
                        } else {
                            break;
                        }
                    }
                    String messageFromClient = null;
                    Socket socket = null;
                    //Listens for client requests
                    try {
                        socket = serverSocket.accept();
                        System.out.println("socket.getChannel()"+socket.getChannel());
                        System.out.println("socket.getLocalPort()"+socket.getLocalPort());
                        System.out.println("socket.getRemoteSocketAddress()"+socket.getRemoteSocketAddress());
                        System.out.println("socket.getInetAddress()"+socket.getInetAddress());
                        System.out.println("socket.getLocalAddress()"+socket.getLocalAddress());
                        System.out.println("socket.getLocalSocketAddress()"+socket.getLocalSocketAddress());
                        System.out.println("socket.socket.getPort()"+socket.getRemoteSocketAddress());
                        //Gets byte stream from client
                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        //Reads and stores the data in a string
                        messageFromClient = bufferedReader.readLine();
                        String[] msgParts = messageFromClient.split("\\|");
                        String currMsgId = msgParts[0];
                        String msg = msgParts[1];
                        System.out.println("msg_id" + currMsgId);
                        System.out.println("Proposing before " + initSeqClient);
                        initSeqClient = initSeqClient + 1;
                        //System.out.println("Proposing adter "+initSeqClient);
                        String seq = Integer.toString(initSeqClient) + "." + mPort;
                        //priorityQueue.add(new Message(msg, Integer.parseInt(currMsgId), Double.parseDouble(seq), false));
                        aList.add(new Message(msg, Integer.parseInt(currMsgId), Double.parseDouble(seq), false));
                        currentPort = currMsgId.substring(0,5);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        //System.out.println("Proposing "+initSeqClient+"."+mPort);
                        printWriter.println(seq);
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException Server1");
                    }catch(NullPointerException e){
                        Log.e(TAG, "Message null");
                    }


                    try {
                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        messageFromClient = bufferedReader.readLine();
                        String[] msgParts = messageFromClient.split("\\|");
                        String msgId = msgParts[1];
                        String finalSeq = msgParts[2];
                        String msg = msgParts[3];
//                        socket.setSoTimeout(500);

                        //System.out.println("Final message received");
                        System.out.println("msgId " + msgId);
                        //System.out.println("finalSeq " + finalSeq);
                        Log.i("Received final message ", msg+" "+msgId);

                        for (Message m : aList) {
                            if (m.getMsg().equals(msg) && msgId.equals(Integer.toString(m.getMsgId()))) {
                                //System.out.println("Changing object params");
                                //Log.d("Changing object params", TAG);
                                //System.out.println("Double parsed "+Double.parseDouble(finalSeq));
                                m.setSeqNum(Double.parseDouble(finalSeq));
                                m.setDeliverable(true);
                            }
                        }

                    } catch (IOException e) {
                        Log.e("ClientTasksocketserver2",TAG);
                    }catch(NullPointerException e){
                        Log.e("Message null exception", TAG);
                        ArrayList<Message> msgToRemove = new ArrayList<Message>();
                        for(Message m: aList){
                            System.out.println("PPPQ"+m.getMsgId()+" "+m.getSeqNum()+" "+m.getMsg()+" "+m.getDeliverable());
                            if(currentPort!=""&&Integer.toString(m.getMsgId()).substring(0,5).equals(currentPort)&&m.getDeliverable()==false){
                                System.out.println("Removing message in catch"+m.getMsgId()+" "+m.getMsg());
                                msgToRemove.add(m);
                                //aList.remove(m);
                            }
                        }
                        aList.removeAll(msgToRemove);
                        msgToRemove.clear();

                    }



                }
            /*} catch (IOException e) {

                Log.e("Server side exception", TAG);

            }*/
            //return null;
        }



        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

                //System.out.println("Inside received final sequence");
                String strReceived = strings[0].trim();
                Log.d(TAG, "msg received"+strReceived);
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append(strReceived + " "+strings[1]+"\t\n");
                /*ContentValues keyValueToInsert = new ContentValues();
                keyValueToInsert.put("key", Integer.toString(msg_count));
                keyValueToInsert.put("value", strReceived);
                msg_count++;
                Log.d(TAG, "key "+msg_count+" Value "+strReceived);
                Uri newUri = getContentResolver().insert(GroupMessengerProvider.providerUri, keyValueToInsert);*/


            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */


    private class ClientTask extends AsyncTask<String, Void, Void> {

        double maxSeqHeard = 0;
        //String remote_ports[] = {REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4, REMOTE_PORT0};
        //Socket[] sockets = new Socket[5];
        //ArrayList<Socket> sockets = new ArrayList<Socket>();
        LinkedHashMap<String, Socket> socketMap = new LinkedHashMap<String, Socket>();




        @Override
        protected Void doInBackground(String... msgs) {
            //try {
                //String remote_ports[] = {REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4, REMOTE_PORT0};
                //sequence.put(mPort, sequence.get(mPort)+1);
                //int st = sequence.get(mPort);
                //st++;
                localMsgId = localMsgId + 1;
                //maxMsgSeq = maxMsgSeq + 1;

                System.out.println("Size is "+rem_ports.size());

                for (int i = 0; i < rem_ports.size(); i++) {
                    if(rem_ports.get(i).equals(REMOVE_PORT) && REMOVE_PORT != ""){
                        System.out.println("Continuing "+rem_ports.get(i));
                        continue;
                    }
                    //1.B-Multicast
                    Socket socket=null;
                    try {
                        socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(rem_ports.get(i)));
                        //sockets.add(socket);
                        socketMap.put(rem_ports.get(i), socket);
                        String msgToSend = msgs[0];
                        StringBuffer sb = new StringBuffer();

                        sb.append(localMsgId + "|");
                        //Log.i("cdscd", initSequence+"."+mPort+"|");
                        //sb.append(initSequence+"."+mPort+"|");
                        sb.append(msgToSend);

                        //Writing the String msgToSend to output stream of data
                        OutputStream outputStream = socket.getOutputStream();
                        //System.out.println("ops "+outputStream);
                        //channelMap.put(remote_ports[i], socket);
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        //System.out.println("aaaaa"+sb.toString());
                        printWriter.println(sb.toString());
                        //Thread.sleep(100);
                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        //Reads and stores the data in a string
                        //String seq_num = bufferedReader.readLine();
                        //System.out.println("seq_num"+seq_num);

                        //Thread.sleep(100);
//                        try {
//                            socket.setSoTimeout(500);
//                            socket.setSoTimeout(500);
                            String messageFromClient = bufferedReader.readLine();
                            //socket.setSoTimeout(500);
                            //System.out.println("messageFromServer  " + messageFromClient);
                            //sequenceList.add(Double.parseDouble(messageFromClient));
                            maxSeqHeard = Math.max(maxSeqHeard, Double.parseDouble(messageFromClient));
//                        } catch (NullPointerException e) {
//                            socket.close();
//                            REMOVE_PORT = rem_ports.get(i);
////                            rem_ports.remove(i);
////                            sockets.remove(i);
//                        }


                        //Thread.sleep(500);
                        //socket.close();}
                    /*}catch(NullPointerException e){
                        System.out.println("Null Received");
                        Log.i("Removing port ", rem_ports.get(i));

                        rem_ports.remove(i);
                    }*/
                    } catch (NullPointerException ex) {
                        if(REMOVE_PORT==""){
                            try{
                            socket.close();}
                            catch(Exception e){
                                Log.e("Unable to close socket", TAG);
                            }
                            System.out.println("Removing port "+rem_ports.get(i));
                            REMOVE_PORT = rem_ports.get(i);
                            socketMap.remove(socketMap.get(REMOVE_PORT));
                        }
                        Log.e(TAG, "ClientTask Null pointer  Exception 1");
                    } catch (SocketException ex) {
                        Log.e(TAG, "ClientTask socket Exception 1");
                    } catch (IOException ex) {
                        Log.e(TAG, "ClientTask socket IOException 1");
                    }

                    //Log.i("Removing port ", rem_ports.get(i));
                        //rem_ports.remove(i);

                    }


                //System.out.println("Max seq heard is "+maxSeqHeard);
            //try{
                for (Socket socket : socketMap.values()) {
                    try {
                    //System.out.println("Inside client remulticasting");
                    //Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            //Integer.parseInt(st));


                    String msgToSend = msgs[0];
                    StringBuffer sb = new StringBuffer();
                    sb.append("final|");
                    sb.append(localMsgId + "|");
                    //System.out.println("db"+maxSeqHeard);
                    //Log.i("mmmmmm", Double.toString(maxSeqHeard));
                    sb.append(Double.toString(maxSeqHeard)+"|");
                    sb.append(msgToSend);
                    Log.i("Sending final message ", msgToSend+" "+localMsgId);
                    //Writing the String msgToSend to output stream of data
                    //OutputStream outputStream = channelMap.get(remote_ports[i]);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    //System.out.println("aaaaa"+sb.toString());
                    printWriter.println(sb.toString());
                    //socket.close();
                    Thread.sleep(100);
                    }catch(IOException e){
                        //Log.i("Removing port ", rem_ports.get(i));
//                        socket.close();
                        //REMOVE_PORT = rem_ports.get(i);

                        Log.e(TAG, "ClientTask socket IOException 2");
                    }catch(InterruptedException e){
                        Log.e(TAG, "Uninterrupted Exception");
                    }
                }
        /*}catch(IOException e){
                Log.i("ddfdaff",TAG);
            }*/


            /*} catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            return null;

        }

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
