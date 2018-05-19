package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by issackoshypanicker on 5/2/18.
 */

public class Message {

    private String type;
    private String myPort;
    private String key;
    private String value;
    private String sendPort;
    private String outStar;


    public Message(String type, String port) {
        this.type = type;
        this.myPort = port;
        this.key ="";
        this.value ="";
        this.sendPort = "";
        this.outStar ="";

    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public String getSendPort() {
        return sendPort;
    }

    public void setSendPort(String sendPort) {
        this.sendPort = sendPort;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOutStar() {
        return outStar;
    }

    public void setOutStar(String outStar) {
        this.outStar = outStar;
    }

    @Override
    public String toString() {
        return type+":"+myPort+":"+key+":"+value+":"+sendPort+":"+outStar;
    }

    public String toString1() {
        return "Message{" +
                "type='" + type + '\'' +
                ", myPort='" + myPort + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", sendport='" + sendPort + '\'' +
                '}';
    }
}


