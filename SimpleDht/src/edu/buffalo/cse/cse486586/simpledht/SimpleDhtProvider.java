package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
	private Uri mUri;
	HashMap<String,Integer> nodePosition=new HashMap<String,Integer>();
	HashMap<Integer,String> nodeReversePosition=new HashMap<Integer,String>();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static final int SERVER_PORT=10000;
	static String nodeId=null;
	static String portStr=null;
	static String myPort=null;
	static int succ_pointer=0;
	static int pred_pointer=0;
	static int position;
	static int count=0;
	String succId=null;
	String predId=null;
	String key_Received;
	String val_Received;
	String lowest;
	boolean loop=true;
	ArrayList<Integer> nodes = new ArrayList<Integer>();
	MatrixCursor localCursor=new MatrixCursor(new String [] {KEY_FIELD,VALUE_FIELD});
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String FILENAME=selection;
		Context context=this.getContext();
		String list[]=context.fileList();
		String ret=String.valueOf(Integer.parseInt(portStr)*2);
		boolean flag=false;
		String msg;
		if(FILENAME.equals("@")){
			//Delete all the files in the local node
			for(int i=0;i<list.length;i++){
				context.deleteFile(list[i]);
			}
		}
		else if(FILENAME.equals("*")){
			//Delete all files in the entire DHT
			for(int i=0;i<list.length;i++){
				context.deleteFile(list[i]);
			}
			if(succ_pointer == 0 || (succ_pointer == 11108 && pred_pointer == 11108 && portStr.equals("5554"))){

			}
			//Send delete message to other nodes in dht
			else{
				String delete="deleteAll_"+ret;
				Thread client=new Thread(new Client(delete,succ_pointer));
				client.start();
			}
		}
		else{
			//Check if the content provider contains the key 
			for(int i=0;i<list.length;i++){
				if(FILENAME.equals(list[i])){
					flag=true;
					break;
				}
			}
			if(flag == true){
				context.deleteFile(FILENAME);
				flag=false;
			}
			else{
				//Forward the query to the next node
				msg="delete_"+FILENAME;
				Thread client=new Thread(new Client(msg,succ_pointer));
				client.start();
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
		String key=null;
		String value=null;
		String keyId=null;
		String msg=null;
		Context context=this.getContext();
		key=values.getAsString(KEY_FIELD);
		value=values.getAsString(VALUE_FIELD);
		//Hash the key
		try {
			keyId=genHash(key);
			//Check if the node is a stand alone node in the system
			//Just insert the keys in that case
			if(succ_pointer == 0 || (succ_pointer == 11108 && pred_pointer == 11108 && portStr.equals("5554"))){
				try {
					FileOutputStream fos=context.getApplicationContext().openFileOutput(key,Context.MODE_PRIVATE);
					fos.write(value.getBytes());
					fos.close();
					Log.v("insert", values.toString());
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//Check if the given key can be inserted in the current node
			else if((keyId.compareTo(predId) > 0 && keyId.compareTo(nodeId) <= 0) || ((keyId.compareTo(predId)>0 && keyId.compareTo(nodeId)>0) && nodeId.equals(genHash(lowest))) || (keyId.compareTo(nodeId) <=0 && portStr.equals(lowest))){
				// Insert the key in the current content provider
				// Write they keyId and value to the internal storage
				try {
					FileOutputStream fos=context.getApplicationContext().openFileOutput(key,Context.MODE_PRIVATE);
					fos.write(value.getBytes());
					fos.close();
					Log.v("insert", values.toString());
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}	
			//else Forward the key to the successor node
			else{
				msg="insert_"+key+"_"+value;
				Thread client=new Thread(new Client(msg,succ_pointer));
				client.start();
			}}
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		nodePosition.put("5554",0);
		nodePosition.put("5558",1);
		nodePosition.put("5560",2);
		nodePosition.put("5562",3);
		nodePosition.put("5556",4);
		nodeReversePosition.put(0,"5554");
		nodeReversePosition.put(1,"5558");
		nodeReversePosition.put(2,"5560");
		nodeReversePosition.put(3,"5562");
		nodeReversePosition.put(4,"5556");
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			nodeId=genHash(portStr);     //Get the hash of the current avd
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		//Create a server thread for all avds to listen to incoming messages
		Thread server=new Thread(new Server());
		server.start();
		if(portStr.equals("5554")){
			succ_pointer=11108;
			pred_pointer=11108;
			succId=nodeId;
			predId=nodeId;
			position=nodePosition.get(portStr);
			count++;
			lowest=portStr;
			nodes.add(position);
		}
		else{
			//Send a join request to 5554 
			//Create a client thread to send the join request
			String msg="join_"+portStr+"_"+nodeId;
			Thread client=new Thread(new Client(msg,11108));
			client.start();
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) {
		String msg;
		String ret=String.valueOf(Integer.parseInt(portStr)*2);
		String FILENAME=selection;
		String VALUE;
		Context context=this.getContext();
		String list[]=context.fileList();
		boolean flag=false;
		MatrixCursor mCursor=new MatrixCursor(new String [] {KEY_FIELD,VALUE_FIELD});
		if(FILENAME.equals("@")){
			//Get all files in the local node
			for(int i=0;i<list.length;i++){
				VALUE=localQuery(list[i]); 
				mCursor.addRow(new String[]{list[i],VALUE});
			}
			return mCursor;
		}
		else if(FILENAME.equals("*")){
			//Get all files in the entire DHT
			for(int i=0;i<list.length;i++){
				VALUE=localQuery(list[i]);
				Log.d("query",list[i]);
				localCursor.addRow(new String[]{list[i],VALUE});
			}
			//If only one node is there in the DHT then return its values
			if(succ_pointer == 0 || (succ_pointer == 11108 && pred_pointer == 11108 && portStr.equals("5554"))){
				return localCursor;
			}
			//Else Send a Request to other nodes for their respective keys
			else{
				msg="queryAll_"+ret;
				Thread client=new Thread(new Client(msg,succ_pointer));
				client.start();
				while(loop == true){

				}
				//Wait for the other nodes to return their values
				loop=true;
				return localCursor;
			}
		}
		//To query for a single key
		else{
			//Check if the content provider contains the key 
			for(int i=0;i<list.length;i++){
				if(FILENAME.equals(list[i])){
					flag=true;
					break;
				}
			}
			//If it contains just return the key
			if(flag == true){
				VALUE=localQuery(FILENAME); 
				mCursor.addRow(new String[]{FILENAME,VALUE});
				flag=false;
				Log.v("query", selection);
				return mCursor;
			}
			else{
				//Forward the query to the next node
				msg="query_"+FILENAME+"_"+ret;
				Thread client=new Thread(new Client(msg,succ_pointer));
				client.start();
				//Wait for the response from the other node
				while(loop == true){

				}
				loop=true;
				mCursor.addRow(new String[]{key_Received,val_Received});
				Log.v("query", selection);
				return mCursor;
			}
		}
	}
	//Function to delete a single file
	public boolean localDelete(String file){
		boolean flag=false;
		Context context=this.getContext();
		String list[]=context.fileList();
		for(int i=0;i<list.length;i++){
			if(file.equals(list[i])){
				context.deleteFile(file);
				flag=true;
			}
		}
		return flag;
	}
	//Function to delete all the files in a node
	public void localDeleteAll(){
		Context context=this.getContext();
		String list[]=context.fileList();
		for(int i=0;i<list.length;i++){
			context.deleteFile(list[i]);
		}
	}
	//Function to query for a single file in the node
	public String localQuery(String key){
		Context context=this.getContext();
		String value=null;
		try {
			FileInputStream fis = context.getApplicationContext().openFileInput(key);       
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));  
			value=br.readLine();
			br.close();                                                          
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}
	//Function to query for all the files in the node
	public String localQueryAll(String msg){
		String reply="";
		String finalReply="";
		String temp;
		Context context=this.getContext();
		String list[]=context.fileList();
		String val;
		String flag="true";
		String send;
		int count=0;
		if(list.length >= 1){
			//Get all files in the local node
			for(int i=0;i<list.length;i++){
				val=localQuery(list[i]); 
				temp=list[i]+"_"+val+"_";
				reply=reply+temp;
				count++;
			}
		}
		//String object containing all the key value pairs
		finalReply=String.valueOf(count)+"_"+reply;
		if(succ_pointer == Integer.parseInt(msg)){
			flag="false";
		}
		send="queryResponseAll_"+flag+"_"+finalReply;
		return send;
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
	//Copied from project 1
	class Server implements Runnable{
		public void run(){
			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				while(true){
					Socket client = serverSocket.accept();
					try{
						BufferedReader is = new BufferedReader(
								new InputStreamReader(client.getInputStream()));
						String msg = is.readLine();
						//Thread that will work with the incoming message
						Thread manage=new Thread(new Manage(msg));
						manage.start();
					}catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					client.close();
				} 
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	class Manage implements Runnable{
		String msg;
		public Manage(String message){
			msg=message;
		}
		public void run(){
			String message[]=msg.split("_");
			String newSuccId;
			String newPredId;
			int newSuccPointer;
			int newPredPointer;
			int newPosition;
			String replyMessage;
			//Handle join request
			if(message[0].equals("join")){
				Log.v("join",msg);
				if(count == 1){
					//This is the first join request
					//update the pointers of 5554
					newPosition=nodePosition.get(message[1]);
					succId=message[2];
					predId=message[2];
					succ_pointer=(Integer.parseInt(message[1])*2);
					pred_pointer=(Integer.parseInt(message[1])*2);
					newSuccId=nodeId;
					newPredId=nodeId;
					newSuccPointer=11108;
					newPredPointer=11108;
					//Find the lowest node in the DHT currently
					try {
						if(genHash(lowest).compareTo(message[2]) > 0){
							lowest=message[1];
						}
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					replyMessage="reply_"+newSuccId+"_"+newPredId+"_"+newSuccPointer+"_"+newPredPointer+"_"+newPosition+"_"+lowest;
					count++;
					nodes.add(newPosition);
					//Send update to joining node
					Thread client1=new Thread(new Client(replyMessage,succ_pointer));
					client1.start();
				}
				else{
					newPosition=nodePosition.get(message[1]);   //Position of the incoming node
					//First find where the new node should be inserted
					String prevAndNext=findPrevAndNext(newPosition);
					String split[]=prevAndNext.split("_");
					int prev=Integer.parseInt(split[0]);
					int next=Integer.parseInt(split[1]);
					String prevNode=nodeReversePosition.get(prev);
					String nextNode=nodeReversePosition.get(next);
					//Now we know which nodes must be updated when inserting the new node.
					//Update the pointers of the new node joining the chord.
					//Find the lowest node in the DHT currently
					try {
						if(genHash(lowest).compareTo(message[2]) > 0){
							lowest=message[1];
						}
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					try {
						newSuccId=genHash(nextNode);
						newPredId=genHash(prevNode);
						newSuccPointer=Integer.parseInt(nextNode)*2;
						newPredPointer=Integer.parseInt(prevNode)*2;
						replyMessage="reply_"+newSuccId+"_"+newPredId+"_"+newSuccPointer+"_"+newPredPointer+"_"+newPosition+"_"+lowest;
						count++;
						nodes.add(newPosition);
						//Send update to joining node
						Thread client2=new Thread(new Client(replyMessage,(Integer.parseInt(message[1])*2)));
						client2.start();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					//Need to update the pointers of previous and next node
					//First update the previous node and need to update only the successor pointers
					newSuccId=message[2];
					newSuccPointer=Integer.parseInt(message[1])*2;
					replyMessage="replys_"+newSuccId+"_"+newSuccPointer+"_"+lowest;
					//Send Update to the node now
					Thread client3=new Thread(new Client(replyMessage,(Integer.parseInt(prevNode)*2)));
					client3.start();
					//Similarly update the next node now
					newPredId=message[2];
					newPredPointer=Integer.parseInt(message[1])*2;
					replyMessage="replyp_"+newPredId+"_"+newPredPointer+"_"+lowest;
					//Send Update to the next node now
					Thread client4=new Thread(new Client(replyMessage,(Integer.parseInt(nextNode)*2)));
					client4.start();
				}
			}
			//Update both the successor and predecessor pointers initially
			//Reply from node 5554
			else if(message[0].equals("reply")){
				succId=message[1];
				predId=message[2];
				succ_pointer=Integer.parseInt(message[3]);
				pred_pointer=Integer.parseInt(message[4]);
				position=Integer.parseInt(message[5]);
				lowest=message[6];
			}
			//Update only the successor id and pointer
			//Reply from 5554
			else if(message[0].equals("replys")){
				succId=message[1];
				succ_pointer=Integer.parseInt(message[2]);
				lowest=message[3];
			}
			//Update only the predecessor id and pointer
			//Reply from 5554
			else if(message[0].equals("replyp")){
				predId=message[1];
				pred_pointer=Integer.parseInt(message[2]);
				lowest=message[3];
			}
			//Check if the incoming message can be inserted in the current node
			else if(message[0].equals("insert")){
				ContentValues cv=new ContentValues();
				cv.put(KEY_FIELD,message[1]);
				cv.put(VALUE_FIELD,message[2]);
				insert(mUri,cv);
			}
			//Handle query Request
			else if(message[0].equals("query")){
				String hash;
				String value;
				String keyAndValue;
				try {
					hash=genHash(message[1]);
					if((hash.compareTo(predId) > 0 && hash.compareTo(nodeId) <= 0) || ((hash.compareTo(predId)>0 && hash.compareTo(nodeId)>0) && nodeId.equals(genHash(lowest))) || (hash.compareTo(nodeId) <=0 && portStr.equals(lowest))){
						//Key present here 
						value=localQuery(message[1]);
						keyAndValue="queryResponse_"+message[1]+"_"+value;
						//Return the keyValue Pair to the node that initiated the request
						Thread client5=new Thread(new Client(keyAndValue,Integer.parseInt(message[2])));
						client5.start();
					}
					else{
						//Forward to the next node
						Thread client6=new Thread(new Client(msg,succ_pointer));
						client6.start();
					}
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			//Handle the query Response
			else if(message[0].equals("queryResponse")){
				key_Received=message[1];
				val_Received=message[2];
				loop=false;     //Condition to exit the infinite loop
			}
			//Handle * query to get all values in the DHT
			else if(message[0].equals("queryAll")){
				String obj=localQueryAll(message[1]);
				Thread client7=new Thread(new Client(obj,Integer.parseInt(message[1])));
				client7.start();
				if (succ_pointer != Integer.parseInt(message[1])){
					//pass the query on to next node
					Thread client8=new Thread(new Client(msg,succ_pointer));
					client8.start();
				}
			}
			//Handle * query Response to get all values in the DHT
			else if(message[0].equals("queryResponseAll")){
				int count =Integer.parseInt(message[2]);
				int i=3;
				//Add the incoming values to a cursor object
				while(count > 0){
					localCursor.addRow(new String[]{message[i],message[i+1]});
					i=i+2;
					count--;
				}
				if(message[1].equals("false")){
					loop=false;
				}
			}
			//Handle delete of a single key
			else if(message[0].equals("delete")){
				String fileToDelete=message[1];
				boolean flag=localDelete(fileToDelete);
				//If row is not deleted forward it to the next node
				if(flag == false){
					Thread client9=new Thread(new Client(msg,succ_pointer));
					client9.start();
				}
			}
			//Handle delete of all the keys in the DHT
			else if(message[0].equals("deleteAll")){
				//Delete all the keys in the local node
				localDeleteAll();
				//Forward the delete msg to other nodes in the DHT
				if(succ_pointer != Integer.parseInt(message[1])){
					Thread client10=new Thread(new Client(msg,succ_pointer));
					client10.start();
				}
			}
		}

		//Function that will return where a node must be inserted
		String findPrevAndNext(int pos){
			String prevAndNext;
			int prev=0,next=0;
			int i=pos;
			while(i>0){
				if(nodes.contains((i-1))){
					prev=i-1;
					break;
				}
				i--;
			}
			int j=pos;
			while(j < 4){
				if(nodes.contains((j+1))){
					next=j+1;
					break;
				}
				j++;
			}
			prevAndNext=String.valueOf(prev)+"_"+String.valueOf(next);
			return prevAndNext;
		}
	}
	//Client class that will send out messages to the server
	//Copied from project 1
	class Client implements Runnable{
		String message;
		int port;
		public Client(String msg,int sendToPort) {
			message=msg;
			port=sendToPort;
		}
		public void run(){
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
						10, 0, 2, 2 }),port);
				DataOutputStream os=new DataOutputStream(socket.getOutputStream());
				OutputStreamWriter out=new OutputStreamWriter(os);
				out.write(message);
				out.close();
				os.close();
				socket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
