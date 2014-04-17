package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	static final String TAG = SimpleDhtActivity.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	static String succ="null";
	static String pred="null";
	static int node_count=0;
	static int prev_node_count=0;
	String myPort="null";
	String flag;
	static String nodeid = null;
	static boolean initiator=false;

	DBHelper db;
	private SQLiteDatabase OurDatabase;

	/*Database declarations*/
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";

	private static final String DATABASE_NAME="SimpleDht.db"; 
	public static final String DATABASE_TABLE = "Messages";

	private static class DBHelper extends SQLiteOpenHelper{

		public DBHelper(Context context) {
			super(context, DATABASE_NAME, null,1);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("CREATE TABLE " + DATABASE_TABLE + " (" +
					KEY_FIELD + " TEXT NOT NULL, " + 
					VALUE_FIELD + " TEXT NOT NULL);");
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		}
	}  

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) 
	{
		Log.v(TAG, " Deleting key :"+selection);
		OurDatabase.delete(DATABASE_TABLE, //table name
				"key='"+selection + "'",  // selections
				null); //selections args  
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	/* This method converts the port number to avd number*/
	public String get_avd_number(String myPort)
	{
		String avd="null";
		if(myPort.equals("11108"))
			avd="5554";
		else if(myPort.equals("11112"))
			avd="5556";
		else if(myPort.equals("11116"))
			avd="5558";
		else if(myPort.equals("11120"))
			avd="5560";
		else if(myPort.equals("11124"))
			avd="5562";
		return avd; 
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key=values.getAsString("key");
		String value=values.getAsString("value");
		try {
			String key_hash=genHash(key);
			
			/*inserts in local DB if only 1 node is present*/
			if(pred.equals(myPort)){
				OurDatabase.insert(DATABASE_TABLE, null, values);
			}

			/*else if value of predecessor is less than the current node*/
			else if((genHash(get_avd_number(pred))).compareTo(genHash(get_avd_number(myPort)))<0){
				
				/*insert in local DB */
				if((key_hash.compareTo(genHash(get_avd_number(myPort)))<=0) && (key_hash.compareTo(genHash(get_avd_number(pred)))>0) ){
					OurDatabase.insert(DATABASE_TABLE, null, values);
				}
				/*forward the object to the successor node*/
				else{
					Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											Integer.parseInt(succ));
					String msg ="data_insert"+"|"+key+"|"+value;
					PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
					seq_out.append(msg);
					seq_out.flush();
					request_socket.close();
				}
			}

			/*else if value of predecessor is more than the current node*/
			else if((genHash(get_avd_number(pred))).compareTo(genHash(get_avd_number(myPort)))>0) {

				/*insert in local DB*/
				if((key_hash.compareTo(genHash(get_avd_number(myPort)))<=0) && (key_hash.compareTo(genHash(get_avd_number(pred)))<0) ){
					OurDatabase.insert(DATABASE_TABLE, null, values);
				}
				/*insert in local DB*/
				else if(key_hash.compareTo(genHash(get_avd_number(myPort)))>0 && (key_hash.compareTo(genHash(get_avd_number(pred)))>0) ){
					OurDatabase.insert(DATABASE_TABLE, null, values);
				}
				/*forward to successor node*/
				else{
					try{
						Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succ));
						String msg ="data_insert"+"|"+key+"|"+value;
						PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
						seq_out.append(msg);
						seq_out.flush();	
						request_socket.close();
					}
					catch(Exception ex){
						Log.e(TAG, "socket exception");
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() 
	{
		Context context = getContext();
		db=new DBHelper(context);
		OurDatabase=db.getWritableDatabase();
		if(db == null)
			return false;

		flag="request"; 

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		succ=myPort;
		pred=myPort;
		try 
		{		
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e){	
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, flag , myPort);
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) 
	{
		initiator=true;
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(DATABASE_TABLE);
		Cursor cursor=null;
		String serialize="";

		/*query local DB if its the only node in the chord*/
		if(succ.equals(myPort)){
			if(selection.equals("*")){	
				cursor=OurDatabase.rawQuery("SELECT * FROM Messages", null);	
			}
			else if(selection.equals("@")){				
				cursor=OurDatabase.rawQuery("SELECT * FROM Messages", null);	
			}
			else{	
				cursor=OurDatabase.rawQuery("SELECT * FROM Messages WHERE key="+"'"+selection+"'", null);	
			}
			initiator=false;
			return cursor;		
		}
		else
		{
			String where="";
			if(!(selection.equals("*") || selection.equals("@"))){
				where="WHERE key = '" + selection +"'";
			}
			cursor=OurDatabase.rawQuery("SELECT * FROM Messages "+where , null);	

			cursor.moveToFirst();
			/*serializing the cursor*/
			while(cursor.isAfterLast()==false)
			{
				String key=cursor.getString(0);
				serialize+=key;
				serialize=serialize+"|";
				String value=cursor.getString(1);
				serialize=serialize+value;
				serialize=serialize+"#";
				cursor.moveToNext();
			}

			DataInputStream input;
			String query_reply;
			Socket request_socket;
			try {
				if(!(selection.equals("@")))
				{
					if(selection.equals("*") || cursor.getCount()==0)
					{
						request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succ));
						String msg ="query"+"|"+ selection+"\n";
						PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
						seq_out.append(msg);
						seq_out.flush();
						
/*waiting from successor to reply*/
						input= new DataInputStream(request_socket.getInputStream());
						query_reply=input.readLine();
						serialize=serialize+query_reply;
						seq_out.close();
						input.close();
						request_socket.close();
					}
				}

			/*reconstructing cursor from String*/	
				String tuple[]=serialize.split("#");
				String col[]={"key","value"};
				MatrixCursor MC=new MatrixCursor(col);
				for(String s:tuple){
					String array[]=s.split("\\|");
					String key = array[0].trim();
					String value = array[1].trim();
					MC.addRow(new String[] {key,value});
				}

				initiator=false;
				return MC;
			}
			catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cursor.moveToFirst();
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

		@SuppressWarnings("deprecation")
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			Socket soc=null;
			DataInputStream input;
			String s;
			while(true)
			{
				try{
					soc=serverSocket.accept();            
					input= new DataInputStream(soc.getInputStream());
					s=input.readLine();
					String seq_request[] = s.split("\\|");

					if(s.contains("query"))
					{
						/*return null is the query request returns to d originator*/
						if(initiator)
						{
							PrintStream seq_out = new PrintStream(soc.getOutputStream());            
							seq_out.append("");
							seq_out.flush();
							seq_out.close();
						}
						/*else query the DB or forward to successor*/
						else
						{	
							SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();						
							Cursor cursor=null;
							String serialize="";
							String selection=seq_request[1];
							String where="";
							if(!(selection.equals("*") || selection.equals("@")))
							{
								where="WHERE key = '" + selection +"'";
							}
							cursor=OurDatabase.rawQuery("SELECT * FROM Messages "+where , null);	
							cursor.moveToFirst();
							
							while(cursor.isAfterLast()==false)
							{
								String key=cursor.getString(0);
								serialize+=key;
								serialize=serialize+"|";
								String value=cursor.getString(1);
								serialize=serialize+value;
								serialize=serialize+"#";
								cursor.moveToNext();
							}

							DataInputStream query_input;
							String query_reply;
							/*forward to succ*/Socket request_socket;

							if(selection.equals("*") || cursor.getCount()==0)
							{
								request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(succ));
								String msg ="query"+"|"+ selection+"\n";

								PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
								seq_out.append(msg);
								seq_out.flush();

								input= new DataInputStream(request_socket.getInputStream());
								query_reply=input.readLine();
								if(query_reply==null)
								{
									query_reply="";	
								}
								serialize=serialize+query_reply;

								seq_out.close();
								input.close();
								request_socket.close();
							}
							serialize=serialize+"\n";
							PrintStream reply_soc=new PrintStream(soc.getOutputStream());
							reply_soc.append(serialize);
							reply_soc.flush();
						}
					}
					/*find location for the node to join the chord*/
					else if(s.contains("request")){
						node_count++;
						find_location(seq_request[1]);
					}
					else if(s.contains("updateboth")){
						pred=seq_request[1];
						succ=seq_request[2];
					}
					else if(s.contains("updatesucc")){
						succ=seq_request[1];
					}
					else if(s.contains("updatepred")){
						pred=seq_request[1];
					}
					else if(s.contains("updatefirstnode")){
						succ=seq_request[1];
						pred=seq_request[1];
					}
					
					/*handles insert request*/
					else if(s.contains("data_insert"))
					{
						String key=seq_request[1];
						String value=seq_request[2];
						try {
							String key_hash=genHash(key);
							if (genHash(get_avd_number(pred)).compareTo(genHash(get_avd_number(myPort)))<0)
							{
								if(key_hash.compareTo(genHash(get_avd_number(myPort)))<=0 && (key_hash.compareTo(genHash(get_avd_number(pred)))>0))
								{
									ContentValues values = new ContentValues();
									values.put("key",key);
									values.put("value",value);
									OurDatabase.insert(DATABASE_TABLE, null, values);
								}
								else
								{
									Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											Integer.parseInt(succ));
									String msg ="data_insert"+"|"+key+"|"+value;
									PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
									seq_out.append(msg);
									seq_out.flush();
									request_socket.close();
								}
							}		
							else if((genHash(get_avd_number(pred)).compareTo(genHash(get_avd_number(myPort))))>0)
							{
								if((key_hash.compareTo(genHash(get_avd_number(myPort)))<=0) && (key_hash.compareTo(genHash(get_avd_number(pred)))<0) )
								{
									ContentValues values = new ContentValues();
									values.put("key",key);
									values.put("value",value);
									OurDatabase.insert(DATABASE_TABLE, null, values);
								}
								else if((key_hash.compareTo(genHash(get_avd_number(myPort)))>0)  && (key_hash.compareTo(genHash(get_avd_number(pred)))>0) )
								{
									ContentValues values = new ContentValues();
									values.put("key",key);
									values.put("value",value);
									OurDatabase.insert(DATABASE_TABLE, null, values);
								}
								else
								{
									try{
										Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
												Integer.parseInt(succ));
										String msg ="data_insert"+"|"+key+"|"+value;
										PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
										seq_out.append(msg);
										seq_out.flush();
										request_socket.close();
									}
									catch (Exception ex)
									{
										Log.e(TAG, "socket exception from client");
									}
								}
							}			
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}						
					}
				}
				catch(IOException e){
					Log.e(TAG, "Error");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}         
			}
		}

		/*calculates the avd_number form port number*/
		public String get_avd_number(String myPort)
		{
			String avd="null";
			if(myPort.equals("11108"))
				avd="5554";
			else if(myPort.equals("11112"))
				avd="5556";
			else if(myPort.equals("11116"))
				avd="5558";
			else if(myPort.equals("11120"))
				avd="5560";
			else if(myPort.equals("11124"))
				avd="5562";
			return avd; 
		}

		/*
		 * finds the location in the the chord for the new node request 
		 * */
		public void find_location(String seq_request) throws SocketException, UnknownHostException, IOException, NoSuchAlgorithmException
		{
			/*For single node 0 */
			if (node_count==1 && seq_request.equals(REMOTE_PORT0)){
				succ = REMOTE_PORT0;  
				pred = REMOTE_PORT0;  
			}
			/*For exactly two nodes*/
			else if (node_count==2 && succ.equals(pred)){
				succ = seq_request;  
				pred = seq_request;
				flag="updatefirstnode";
				send(flag, seq_request, myPort);
			}
			else
			{		
				String myhash=genHash(get_avd_number(myPort));	
				String request_hash=genHash(get_avd_number(seq_request));
				String succ_hash=genHash(get_avd_number(succ));
				String pred_hash=genHash(get_avd_number(pred));
				if((request_hash.compareTo(myhash)>0)&&(request_hash.compareTo(succ_hash)<0))
				{
					String temp=succ;
					flag="updateboth";
					send(flag, seq_request, myPort, temp);

					flag="updatepred";
					send(flag, succ, seq_request);

					succ =seq_request;
				}				
				else if((request_hash.compareTo(myhash)>0)&&(succ_hash.compareTo(myhash)<0))
				{
					String temp=succ;
					succ=seq_request;
					flag="updateboth";
					send(flag,seq_request,myPort,temp);

					flag="updatepred";
					send(flag, temp, seq_request);
				}
				else if((request_hash.compareTo(myhash)<0)&&(request_hash.compareTo(succ_hash)<0)&&(succ_hash.compareTo(myhash)<0))
				{
					String temp=succ;
					succ=seq_request;
					flag="updatepred";
					send(flag,temp,seq_request);

					flag="updateboth";
					send(flag, seq_request, myPort, temp);
				}
				else 
				{
					flag="forward";
					send(flag,succ,seq_request);
				}
			}
		}

	/*send msg to the predecessor/successor*/	
		public void send(String... send) throws SocketException, UnknownHostException, IOException
		{
			String request_port=send[1];
			Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(request_port));
			if(send[0].equals("forward"))
			{
				String msg ="request"+"|"+send[2];
				PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
				seq_out.append(msg);
				seq_out.flush();					
			}

			if(send[0].equals("updateboth"))
			{
				String msg= send[0]+"|"+send[2]+"|"+send[3];
				PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
				seq_out.append(msg);
				seq_out.flush();	
			}

			if(send[0].equals("updatesucc"))
			{
				String msg= send[0]+"|"+send[2];
				PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
				seq_out.append(msg);
				seq_out.flush();		
			}

			if(send[0].equals("updatepred"))
			{
				String msg= send[0]+"|"+send[2];
				PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
				seq_out.append(msg);
				seq_out.flush();	
			}

			if(send[0].equals("updatefirstnode"))
			{
				String msg= send[0]+"|"+send[2];
				PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
				seq_out.append(msg);
				seq_out.flush();	
			}
			request_socket.close();
		}

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		protected void onProgressUpdate(String...strings) {
			return;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				if(msgs[0].contains("request")){
					String request_port=REMOTE_PORT0;
					Socket request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(request_port));
					String msg= msgs[0]+"|"+msgs[1];
					PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
					seq_out.append(msg);
					seq_out.flush();
					request_socket.close();
				}
				else{
					return null;
				}
			} catch (UnknownHostException e) {
			} catch (IOException e) {
			}
			return null;
		}
	}
}