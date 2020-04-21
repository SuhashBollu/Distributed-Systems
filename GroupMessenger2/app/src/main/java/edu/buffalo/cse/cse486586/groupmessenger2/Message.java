package edu.buffalo.cse.cse486586.groupmessenger2;
import java.lang.Double.*;
public class Message implements Comparable<Message>{

    private String msg;
    private int msgId;
    private double seqNum;
    private boolean deliverable;

    public Message(String msg, int msgId, double seqNum, boolean deliverable){
        this.msg = msg;
        this.msgId = msgId;
        this.seqNum = seqNum;
        this.deliverable = deliverable;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public double getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(Double seqNum) {
        this.seqNum = seqNum;
    }

    public boolean getDeliverable() {
        return deliverable;
    }

    public void setDeliverable(boolean deliverable) {
        this.deliverable = deliverable;
    }

    @Override
    public int compareTo(Message another) {
        //return this.getSeqNum()<another.getSeqNum()?this.getSeqNum():another.getSeqNum();
        //return Integer.compare(this.getSeqNum(), another.getSeqNum());
        Double num1 = new Double(this.getSeqNum());
        Double num2 = new Double(another.getSeqNum());
        //int result = num1.compareTo(num2);
        if (num1 < num2)
            return -1;
        else if (num1 > num2)
            return 1;
        return 0;
    }
        //return result;

    @Override
    public String toString(){
        return this.getMsgId()+" "+this.getMsg()+" "+this.getSeqNum()+" "+this.getDeliverable();
    }

}
