package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static final int SERVER_PORT = 10000;

    public String mPort = "";
    public String mPortNum = "";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

//        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
//        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        Log.v(TAG, "portNum "+portStr);
//        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
//        Log.v(TAG, "myPort "+myPort);
//        mPortNum = portStr;
//        mPort = myPort;
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
