package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by surabhi on 4/9/17.
 */

public class Node {
    String selfPort;
    String predPort;
    String sucPort;
    String selfHash;
    String predHash;
    String sucHash;
    String nodeType;
    String destination;
    String key;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    String value;

    final String delimiter= "::::::::";
    //Types of nodes: Join, insert, query, delete, sucUpdtae, predUpdate

    public Node(String selfPort, String nodeType, String sucPort, String predPort, String selfHash, String predHash, String sucHash, String destination, String key, String value) {
        this.selfPort = selfPort;
        this.nodeType = nodeType;
        this.sucPort = sucPort;
        this.predPort = predPort;
        this.selfHash = selfHash;
        this.predHash = predHash;
        this.sucHash = sucHash;
        this.key = key;
        this.value=value;
        this.destination=destination;

    }
    public Node(String str){
        String[] parts=str.split(delimiter);
        this.selfPort = parts[0];
        this.nodeType = parts[1];
        this.sucPort = parts[2];
        this.predPort = parts[3];
        this.selfHash = parts[4];
        this.predHash = parts[5];
        this.sucHash = parts[6];
        this.key =parts[7];
        this.value=parts[8];
        this.destination=parts[9];
    }

    public String getSucHash() {
        return sucHash;
    }

    public void setSucHash(String sucHash) {
        this.sucHash = sucHash;
    }

    public String getSelfHash() {
        return selfHash;
    }

    public void setSelfHash(String selfHash) {
        this.selfHash = selfHash;
    }

    public String getPredHash() {
        return predHash;
    }

    public void setPredHash(String predHash) {
        this.predHash = predHash;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getSucPort() {
        return sucPort;
    }

    public void setSucPort(String sucPort) {
        this.sucPort = sucPort;
    }

    public String getPredPort() {
        return predPort;
    }

    public void setPredPort(String predPort) {
        this.predPort = predPort;
    }

    public String getSelfPort() {
        return selfPort;
    }

    public void setSelfPort(String selfPort) {
        this.selfPort = selfPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String nodeToString(){
        String toReturn=selfPort + delimiter + nodeType + delimiter + sucPort + delimiter + predPort + delimiter + selfHash + delimiter + predHash + delimiter + sucHash + delimiter + key + delimiter + value +delimiter + destination;;
        return toReturn;
    }


}
