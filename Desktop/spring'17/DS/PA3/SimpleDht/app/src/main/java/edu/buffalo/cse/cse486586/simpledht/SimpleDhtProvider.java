package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String selfPort;
    String selfHash;
    int firstPort=11108;
    Node selfNode;
    ArrayList<String> nodeHashArrayList=new ArrayList<String>();
    HashMap<String, String> nodeHashMap=new HashMap<String, String>();
    ArrayList<String> insertedKeys = new ArrayList<String>();
    public static final Uri mUri = Uri.parse("content://"+ "edu.buffalo.cse.cse486586.simpledht.provider");
    final String delimiterself= ",";
    final String delimiterperNode=":::";
    public void fileInput(String key, String value){
        try {
            FileOutputStream outputStream;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            insertedKeys.add(key);
            Log.d("inserted key", key+ " "+value);
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }
    }

    public void fileDelete(String key){
        try {
            File file = getContext().getFileStreamPath(key);
            if (file.delete()) {
                insertedKeys.remove(key);
                Log.d("Deleted", key);
            }
        }catch (Exception e) {
            Log.e(TAG, "File read failed");
            e.printStackTrace();
        }
    }
    public String fileOutput(String key){
        String msg = null;
        try {
            FileInputStream inputStream = getContext().openFileInput(key);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            msg = bufferedReader.readLine();
            bufferedReader.close();
        } catch (Exception e) {
            Log.e(TAG, "File read failed");
//            e.printStackTrace();
        }
        return msg;
    }
    public boolean isInSelfRegion(String keyHash){
        if(keyHash.compareTo(selfNode.getSelfHash())<=0 && keyHash.compareTo(selfNode.getPredHash()) > 0){
            return true;
        }
        else return false;
    }
    public boolean isInLastRegion(String keyHash){
        if(keyHash.compareTo(selfNode.getSelfHash())<=0 || keyHash.compareTo(selfNode.getPredHash()) > 0){
            return true;
        }
        else return false;
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
       //if succPort == selfPort then always local delete
        if(selection.equals("*")){

            if(selfNode.selfPort.equals(selfNode.sucPort)){
                selection="@";
            }
            else{
                Node deleteNode=new Node(selfPort, "Delete", selfPort, selfPort, null, selfHash,
                        selfHash,  selfNode.getSucPort(), selection, null);
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteNode.nodeToString()).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                selection="@";
            }
        }
        if(selection.equals("@")){
            for(String key : insertedKeys){
                fileDelete(key);
            }

        }
        else{
            if(selfNode.selfPort.equals(selfNode.sucPort)) {
                fileDelete(selection);
            }
            if(insertedKeys.contains(selection)){
                fileDelete(selection);
            }
            else{
                //look in succ node;
                String keyHash= null;
                try {
                    keyHash = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                Node deleteNode=new Node(selfPort, "Delete", selfPort, selfPort, keyHash, selfHash,
                        selfHash,  selfNode.getSucPort(), selection, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteNode.nodeToString());
                Log.d("Sending to next node" ,selfNode.getSucPort());
            }

        }
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
        //if succPort == selfPort then always local insert
        String key= values.get("key").toString();
        String value=values.get("value").toString();
        if(selfNode.selfPort.equals(selfNode.sucPort)) {
            fileInput(key, value);
            Log.v("insert", value);
        }
        else{
            String keyHash = null;
            try {
                keyHash=genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if(isInSelfRegion(keyHash)){
                Log.d("INSERTING SELF",key+" "+keyHash);
                fileInput(key, value);
                Log.v("insert", value);
            }
            else if((selfHash.compareTo(selfNode.getPredHash())<0) && isInLastRegion(keyHash)){
                Log.d("LAST REGION INSERT", key+" "+keyHash);
                fileInput(key, value);
                Log.v("insert", value);
            }
            else{
                Node insertNode=new Node(selfPort, "Insert", selfPort, selfPort, keyHash, selfHash,
                        selfHash,  selfNode.getSucPort(), key, value);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertNode.nodeToString());
                Log.d("Sending to next node" ,key+" "+keyHash+" "+selfNode.getSucPort());
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //super.onCreate(savedInstanceState);
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        selfPort = String.valueOf(Integer.parseInt(portStr) * 2);

        try {
            selfHash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
        }
        Log.e("port", selfPort);
        Log.e("hash",selfHash);
        Log.e("PORTS", selfPort + " " + String.valueOf(firstPort) + " " +
                String.valueOf(selfPort.equals(String.valueOf(firstPort))));
        if(!selfPort.equals(String.valueOf(firstPort))){
            Log.e("JOINING", String.valueOf(firstPort));
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            selfNode = new Node(selfPort, "Join", selfPort, selfPort, selfHash, selfHash,
                    selfHash, String.valueOf(firstPort), null, null);
            String nodeToJoin=selfNode.nodeToString();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeToJoin);
        }
        else {
            Log.e("not JOINING"," first");
            selfNode = new Node(selfPort, "First", selfPort, selfPort, selfHash, selfHash,
                    selfHash, selfPort, null, null);
            nodeHashArrayList.add(selfHash);
            nodeHashMap.put(selfHash,String.valueOf(selfPort));
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        //if succPort == selfPort then always local query
        String[] columns = {"key", "value"};
        MatrixCursor msgCursor = new MatrixCursor(columns);
        if(selection.equals("*")){
            if (selfNode.selfPort.equals(selfNode.sucPort)) {
                selection = "@";
            }
            else{
                String recievedString=null;
                Node QueryNode=new Node(selfPort, "Query", selfPort, selfPort, null, selfHash,
                        selfHash,  selfNode.getSucPort(), selection, null);
                try {
                    recievedString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QueryNode.nodeToString()).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                if(recievedString != null){
                    String[] parts=recievedString.split(delimiterperNode);
                    //Log.d("Rows returned ", String.valueOf(parts.length));
                    for(String key : parts){
                        if(!key.equals("")){
                            String[] keyValuePair=key.split(delimiterself);
                            msgCursor.addRow(new String[]{keyValuePair[0], keyValuePair[1]});
                        }

                    }
                    Log.e("Length", String.valueOf(msgCursor.getCount()));
                }
                selection="@";
                Log.d("Sending to next node" ,selfNode.getSucPort());
            }
        }
        if(selection.equals("@")){
            Log.e("QUERY", android.text.TextUtils.join(" ",insertedKeys));
            String msg = null;

                for(String key : insertedKeys){
                    msg=fileOutput(key);
                    msgCursor.addRow(new String[]{key, msg});
                    Log.v("query", key);
                }
        }
        else {
            if (selfNode.selfPort.equals(selfNode.sucPort)) {
                String msg = fileOutput(selection);
                msgCursor.addRow(new String[]{selection, msg});
                Log.v("query", selection);
            }
            else if(insertedKeys.contains(selection)){
                String msg =fileOutput(selection);
                msgCursor.addRow(new String[]{selection, msg});
                Log.v("query", selection);
            }
            else{

                String recievedString=null;
                Node QueryNode=new Node(selfPort, "Query", selfPort, selfPort, null, selfHash,
                        selfHash,  selfNode.getSucPort(), selection, null);
                try {
                    recievedString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QueryNode.nodeToString()).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                if(recievedString != null){
                    QueryNode = new Node(recievedString);
                    msgCursor.addRow(new String[]{selection, QueryNode.getValue()});
                }
                Log.d("Sending to next node" ,selfNode.getSucPort());
            }
        }
        return msgCursor;
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
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = null;
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
            uriBuilder.scheme("content");
            Uri mUri = uriBuilder.build();
            try {
                while (true) {
                    socket = serverSocket.accept();
                    DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                    String recievedMsg = dataIn.readUTF();
                    DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                    Node recieved = new Node(recievedMsg);
                    //Join node
                    if(recieved.getNodeType().equals("Join")){
                        nodeHashArrayList.add(recieved.getSelfHash());
                        nodeHashMap.put(recieved.getSelfHash(),recieved.getSelfPort());
                        Collections.sort(nodeHashArrayList);
                        int nodeIndex=nodeHashArrayList.indexOf(recieved.getSelfHash());
                        int predIndex;
                        int sucIndex;
                        if(nodeIndex==0){ // node is at head
                            predIndex=nodeHashArrayList.size() - 1;
                            sucIndex=nodeIndex+1;
                        }

                        else if(nodeIndex == (nodeHashArrayList.size() - 1)){ //node is at tail
                            sucIndex=0;
                            predIndex=nodeIndex-1;
                        }

                        else{
                            predIndex=nodeIndex-1;
                            sucIndex=nodeIndex+1;
                        }
                        recieved.setSucPort(nodeHashMap.get(nodeHashArrayList.get(sucIndex)));
                        recieved.setPredPort(nodeHashMap.get(nodeHashArrayList.get(predIndex)));
                        recieved.setSucHash(nodeHashArrayList.get(sucIndex));
                        recieved.setPredHash(nodeHashArrayList.get(predIndex));
                        String toSend=recieved.nodeToString();
                        dataOut.writeUTF(toSend);
                    }
                    //succsessor update
                    else if(recieved.getNodeType().equals("Suc Update")){
                        selfNode.setSucPort(recieved.getSelfPort());
                        selfNode.setSucHash(recieved.getSelfHash());
                        Log.e("Neighbours suc", selfNode.getPredPort() + " " + selfNode.getSelfPort()
                                + " " + selfNode.getSucPort());
                    }
                    //predecessor update
                    else if(recieved.getNodeType().equals("Pred Update")){
                        selfNode.setPredPort(recieved.getSelfPort());
                        selfNode.setPredHash(recieved.getSelfHash());
                        Log.e("Neighbours pred", selfNode.getPredPort() + " " + selfNode.getSelfPort()
                                + " " + selfNode.getSucPort());
                    }
                    //Inserting keys
                    else if(recieved.getNodeType().equals("Insert")){
                        Log.d("Insert Node recieved", recieved.getKey() + " " + recieved.getSelfHash());
                        if(isInSelfRegion(recieved.getSelfHash()) ||
                                (selfHash.compareTo(selfNode.getPredHash())<0 && isInLastRegion(recieved.getSelfHash()))){
                            recieved.setNodeType("Inserted");
                            Log.e("got from "+recieved.selfPort, "putting at "+recieved.destination);
                            fileInput(recieved.getKey(),recieved.getValue());
                            recieved.setSucPort(selfNode.getSucPort());
                            recieved.setPredPort(selfNode.getPredPort());
                            recieved.setSucHash(selfNode.getSucHash());
                            recieved.setPredHash(selfNode.getPredHash());
                        }
                        else{
                            Log.e("SENDING KEY SUCC",recieved.getKey()+" "+recieved.getSelfHash()+" "+selfNode.getSucPort());
                            recieved.setDestination(selfNode.getSucPort());
                            String toSend=recieved.nodeToString();
                            Socket socketSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(recieved.getDestination()));
                            DataOutputStream dataOutSucc = new DataOutputStream(socketSucc.getOutputStream());
                            dataOutSucc.writeUTF(toSend);
                            socketSucc.close();
                        }
                    }
                    //Querying keys
                    else if(recieved.getNodeType().equals("Query")){
                        Log.d("Query Node recieved", recieved.getKey()+" "+recieved.getSelfHash());
                        if(insertedKeys.contains(recieved.getKey())) {
                            recieved.setNodeType("Queried");
                            Log.e("got from "+recieved.selfPort, "found at "+recieved.destination);
                            recieved.setValue(fileOutput(recieved.getKey()));
                            recieved.setSucPort(selfNode.getSucPort());
                            recieved.setPredPort(selfNode.getPredPort());
                            recieved.setSucHash(selfNode.getSucHash());
                            recieved.setPredHash(selfNode.getPredHash());
                            dataOut.writeUTF(recieved.nodeToString());
                        }
                        else if(recieved.getKey().equals("*")){
                            String recievedString = "";
                            if(!recieved.getSelfPort().equals(selfNode.sucPort)){
                                Log.e("STAR IN KEY SUCC",recieved.getKey()+" "+recieved.getSelfHash() +" "+selfNode.getSucPort());
                                recieved.setDestination(selfNode.getSucPort());
                                String toSend=recieved.nodeToString();
                                Socket socketSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(recieved.getDestination()));
                                DataOutputStream dataOutSucc = new DataOutputStream(socketSucc.getOutputStream());
                                dataOutSucc.writeUTF(toSend);
                                DataInputStream dataInSucc = new DataInputStream(socketSucc.getInputStream());
                                recievedString = dataInSucc.readUTF();
                                socketSucc.close();
                            }
                            for(String key : insertedKeys){
                                recievedString+=key+delimiterself+fileOutput(key);
                                recievedString+=delimiterperNode;
                            }
                            dataOut.writeUTF(recievedString);
                        }
                        else{
                            Log.e("LOOKING IN KEY SUCC",recieved.getKey()+" "+recieved.getSelfHash()+" "+selfNode.getSucPort());
                            recieved.setDestination(selfNode.getSucPort());
                            String toSend=recieved.nodeToString();
                            Socket socketSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(recieved.getDestination()));
                            DataOutputStream dataOutSucc = new DataOutputStream(socketSucc.getOutputStream());
                            dataOutSucc.writeUTF(toSend);
                            DataInputStream dataInSucc = new DataInputStream(socketSucc.getInputStream());
                            String recievedString = dataInSucc.readUTF();
                            socketSucc.close();
                            dataOut.writeUTF(recievedString);
                        }
                    }
                    //Deleting keys
                    else if(recieved.getNodeType().equals("Deleted")){
                        Log.d("Delete Node recieved", String.valueOf(isInSelfRegion(recieved.getSelfHash())));
                        if(insertedKeys.contains(recieved.getKey())){
                            recieved.setNodeType("Deleted");
                            Log.e("got from "+recieved.selfPort, "deleting at "+recieved.destination);
                            fileDelete(recieved.getKey());
                            recieved.setSucPort(selfNode.getSucPort());
                            recieved.setPredPort(selfNode.getPredPort());
                            recieved.setSucHash(selfNode.getSucHash());
                            recieved.setPredHash(selfNode.getPredHash());
                        }
                        else if(recieved.getKey().equals("*")){
                            recieved.setDestination(selfNode.getSucPort());
                            String toSend=recieved.nodeToString();
                            Socket socketSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(recieved.getDestination()));
                            DataOutputStream dataOutSucc = new DataOutputStream(socketSucc.getOutputStream());
                            dataOutSucc.writeUTF(toSend);
                            for(String key : insertedKeys){
                                fileDelete(key);
                            }
                        }
                        else{
                            Log.e("SENDING KEY SUCC",recieved.getKey()+" "+recieved.getSelfHash()+" "+selfNode.getSucPort());
                            recieved.setDestination(selfNode.getSucPort());
                            String toSend=recieved.nodeToString();
                            Socket socketSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(recieved.getDestination()));
                            DataOutputStream dataOutSucc = new DataOutputStream(socketSucc.getOutputStream());
                            dataOutSucc.writeUTF(toSend);
                            socketSucc.close();
                        }
                    }

                }
            }catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            Node received=new Node(msgs[0]);
            Log.e("nodeType", received.getNodeType());
            if(received.getNodeType().equals("Join")){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(received.getDestination()));
                    DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                    dataOut.writeUTF(received.nodeToString());
                    DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                    String recievedMsg = dataIn.readUTF();
                    socket.close();
                    selfNode=new Node(recievedMsg);
                    Node sendToSuc=new Node(selfNode.nodeToString());
                    sendToSuc.setNodeType("Pred Update");
                    Node sendToPred=new Node(selfNode.nodeToString());
                    sendToPred.setNodeType("Suc Update");
                    Log.e("Neighbours", selfNode.getPredPort() + " " + selfNode.getSelfPort() + " " + selfNode.getSucPort());
                    //send to suc
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sendToSuc.getSucPort()));
                    dataOut = new DataOutputStream(socket.getOutputStream());
                    dataOut.writeUTF(sendToSuc.nodeToString());
                    socket.close();
                    //send to pred
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sendToPred.getPredPort()));
                    dataOut = new DataOutputStream(socket.getOutputStream());
                    dataOut.writeUTF(sendToPred.nodeToString());
                    socket.close();


                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
//                    Log.e(TAG, "ClientTask socket IOException " );
//                    e.printStackTrace();
                }

            }
//
            if(received.getNodeType().equals("Insert")) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(received.getDestination()));
                    DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                    //dataOut = new DataOutputStream(socket.getOutputStream());
                    dataOut.writeUTF(received.nodeToString());
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
//                    Log.e(TAG, "ClientTask socket IOException " );
//                    e.printStackTrace();
                }
            }
            if(received.getNodeType().equals("Query")){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(received.getDestination()));
                    DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                    //dataOut = new DataOutputStream(socket.getOutputStream());
                    dataOut.writeUTF(received.nodeToString());
                    DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                    String recievedString = dataIn.readUTF();
                    Log.e("CLIENT RECVD", recievedString);
                    return recievedString;
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
//                    Log.e(TAG, "ClientTask socket IOException " );
//                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}
