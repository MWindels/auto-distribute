/**
 * Defines a thread-safe Socket Pool type.
 *
 * Author: Michael Windels.
 */
module distribute.pool;

import std.socket;
import std.concurrency;
import core.time : dur, MonoTime;
import std.algorithm.iteration : map, filter;
import std.typecons : Tuple;
import std.array : array;

package:

/**
 * A pool of re-usable TCP sockets.
 * This type is used to initiate connections.
 */
class ConnectionPool
{
	private shared Object _poolLock;
	private shared (Socket[])[InternetAddress] _pool;
	
	/**
	 *
	 */
	this()
	{
		//have this type occaisionally go through the pool and prune idle sockets
	}
	
	/**
	 *
	 */
	~this()
	{
		
	}
	
	/**
	 * Grabs a connection and passes it to a delegate so it can use that open connection.
	 * The delegate must not close the connection.
	 *
	 * Params:
	 * 	addr = The address to connect to.
	 * 	dl = A delegate which operates on an open connection.
	 */
	public void perform(InternetAddress addr, void delegate(shared Socket) dl)
	{
		
	}
}

/**
 * An endpoint for any number of ConnectionPools on a single machine.
 * This type is used to accept and process connections.
 */
shared class TerminalPool
{
	private immutable Tid _listener;
	
	private shared Object _closeLock;
	private shared bool _closing = false;
	
	/**
	 * Creates a new TerminalPool.
	 * Note that interv should generally be fairly short so recently-used connections can be freed up regularly.
	 *
	 * Params:
	 * 	port = The port on which to listen for connections.
	 * 	conns = The highest number of concurrent connections expected.
	 * 	interv = The number of milliseconds in a select interval.
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 * 	proc = The delegate which handles data received from an open connection.
	 */
	this(ushort port, uint conns, uint interv, uint idleThresh, shared void delegate(shared Socket) shared proc)
	{
		_closeLock = new Object();
		_listener = cast(immutable)spawn(&_listen, port, conns, interv, idleThresh, proc);
	}
	
	/**
	 * Closes a TerminalPool and all threads spawned by it.
	 * The user must call this before the object is destroyed.
	 * Because this allocates memory so it cannot be called in the destructor.
	 */
	public void close()
	{
		synchronized(_closeLock)
		{
			if(_closing)
			{
				return;	//Prevents double-closing.
			}
			else
			{
				_closing = true;
			}
		}
		
		bool waiting = true;
		shared(Tid)[] tidResends = [];
		shared(Variant)[] varResends = [];
		while(waiting)
		{
			receive(
				(Tid tid)
				{
					if(tid != _listener)
					{
						tidResends ~= [cast(shared)tid];
					}
					else
					{
						waiting = false;
					}
				},
				(Variant var)
				{
					varResends ~= [cast(shared)var];
				}
			);
		}
		foreach(tid; tidResends)
		{
			spawn(() shared {send(ownerTid, cast(Tid)tid);});
		}
		foreach(var; varResends)
		{
			spawn(() shared {send(ownerTid, cast(Variant)var);});
		}
	}
	
	/**
	 * Listens for incoming connections and processes them.
	 *
	 * Params:
	 * 	port = The port on which to listen for connections.
	 * 	conns = The highest number of concurrent connections expected.
	 * 	interv = The number of milliseconds in a select interval.
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 * 	proc = The delegate which handles data received from an open connection.
	 */
	private void _listen(ushort port, uint conns, uint interv, uint idleThresh, shared void delegate(shared Socket) shared proc) shared
	{
		//Build an connection-accepting socket.
		Socket listener = new Socket(AddressFamily.INET, SocketType.STREAM, ProtocolType.TCP);
		listener.bind(new InternetAddress("localhost", port));
		listener.listen(conns);
		scope(exit) listener.close();
		
		//Keep track of which sockets are free.
		//Note: StampedSocket stores shared Sockets, but they're only ever accessed from one thread while they're in the free array (hence why shared is cast away so often).
		alias StampedSocket = Tuple!(shared Socket, MonoTime);
		SocketSet readable = new SocketSet();
		StampedSocket[] free = [];
		
		//Keep track of which sockets were recently used.
		shared Object recentLock = new Object();
		shared Socket[] recent = [];
		
		//Keep track of how many sockets are busy.
		shared Object busyLock = new Object();
		shared uint busy = 0;
		
		//Keep track of whether this thread is terminating or not.
		shared Object exitLock = new Object();
		shared bool exiting = false;
		
		//Wrap the proc delegate with some house-keeping code.
		void delegate(shared Socket) shared wrappedProcess = (shared Socket conn) shared
		{
			proc(conn);
			
			synchronized(recentLock)
			{
				recent ~= [conn];
			}
			
			bool teardown = false;
			synchronized(busyLock)
			{
				synchronized(exitLock)
				{
					busy = busy - 1;
					if(exiting && busy == 0)
					{
						teardown = true;
					}
				}
			}
			if(teardown)
			{
				send!ubyte(ownerTid, 0);
			}
		};
		
		//On exit, close all open connections.
		scope(exit)
		{
			scope(exit) send(ownerTid, thisTid);
			
			bool stillBusy = false;
			synchronized(busyLock)
			{
				synchronized(exitLock)
				{
					exiting = true;
					if(busy > 0)
					{
						stillBusy = true;
					}
				}
			}
			if(stillBusy)
			{
				receiveOnly!ubyte();
			}
			
			foreach(conn; free)
			{
				(cast(Socket)conn[0]).close();
			}
			synchronized(recentLock)
			{
				foreach(conn; recent)
				{
					(cast(Socket)conn).close();	//No one else should be able to access these by this point, so casting away shared should be fine.
				}
			}
		}
		
		//Process connections.
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			//Move all recently-freed sockets back to the free pool.
			synchronized(recentLock)
			{
				MonoTime now = MonoTime.currTime;
				free ~= map!(
					(shared Socket conn)
					{
						return StampedSocket(conn, now);
					}
				)(recent).array;	//Using ~= to append an array() isn't the most efficient way to append a large Range, but recent is likely small anyways.
				recent = [];
			}
			
			//Re-build the set of sockets to be read from.
			readable.reset();	//Faster than calling isset() for all members?
			readable.add(listener);
			foreach(conn; free)
			{
				readable.add(cast(Socket)conn[0]);
			}
			
			//Process data from all sockets.
			//Note: The timeout must be short enough that recently-freed sockets can find their way back into the set before they're used again.
			int updates = Socket.select(readable, null, null, dur!"msecs"(interv));
			if(updates > 0)
			{
				if(readable.isSet(listener) != 0)
				{
					synchronized(busyLock)
					{
						busy = busy + 1;
					}
					
					shared Socket newConn = cast(shared)listener.accept();
					spawn(wrappedProcess, newConn);
				}
				
				uint[] activeIdxes = [];
				foreach(idx, conn; free)
				{
					//It's kind of ridiculous that we can't iterate over a SocketSet.  We have the means, it's not like this is C!
					if(readable.isSet(cast(Socket)conn[0]) != 0)
					{
						synchronized(busyLock)
						{
							busy = busy + 1;
						}
						
						activeIdxes ~= [idx];
						spawn(wrappedProcess, conn[0]);
					}
				}
				for(int i = 0, j = 0; i < free.length - activeIdxes.length;)
				{
					if(j < activeIdxes.length && i + j == activeIdxes[j])
					{
						j += 1;
					}
					else
					{
						if(j > 0)
						{
							free[i] = free[i + j];
						}
						i += 1;
					}
				}
				free = free[0 .. ($ - activeIdxes.length)];
			}
			else if(updates == 0)
			{
				MonoTime now = MonoTime.currTime;
				free = filter!(
					(ref StampedSocket conn)
					{
						if(now - conn[1] >= dur!"seconds"(idleThresh))
						{
							(cast(Socket)conn[0]).close();
							return false;
						}
						else
						{
							return true;
						}
					}
				)(free).array;
			}
		}
	}
}
unittest
{
	import core.thread : Thread;
	
	immutable uint num = 10;
	immutable uint size = 16;	//If this is lower than num, then we'll have duplicate sendBuffers.
	shared Tid mainThread = cast(shared)thisTid;
	
	Socket[num] conns;
	ubyte[size][num] sendBuffers;
	for(int i = 0; i < num; ++i)
	{
		conns[i] = new Socket(AddressFamily.INET, SocketType.STREAM, ProtocolType.TCP);
		foreach(j; 0 .. size)
		{
			sendBuffers[i][j] = (i + j) % size;
		}
	}
	
	shared Object recvLock = new Object();
	shared ubyte[size][] recvBuffers = [];
	shared (void delegate(shared Socket) shared) recv =
	(shared Socket shConn) shared
	{
		Socket conn = cast(Socket)shConn;
		shared ubyte[size] interimBuffer;
		conn.receive(cast(ubyte[size])interimBuffer);
		
		synchronized(recvLock)
		{
			recvBuffers = recvBuffers ~ interimBuffer;
		}
		
		send!ubyte(cast(Tid)mainThread, 0);
	};
	
	shared TerminalPool term = new shared TerminalPool(32856, 10, 100, 3, recv);
	scope(exit) term.close();
	
	foreach(conn; conns)
	{
		conn.connect(new InternetAddress("localhost", 32856));
	}
	scope(exit)
	{
		foreach(conn; conns)
		{
			conn.close();
		}
	}
	
	foreach(i, conn; conns)
	{
		conn.send(sendBuffers[i]);
	}
	
	for(int i = 0; i < num; ++i)
	{
		assert(receiveTimeout(dur!"seconds"(15), (ubyte) {}), "TerminalPool timed out.");
		Thread.sleep(dur!"msecs"(500));
		
		bool hasMatch = false;	//The recvBuffers won't necessarily be ordered.
		foreach(sendBuffer; sendBuffers)
		{
			if(cast(ubyte[size])recvBuffers[i] == sendBuffer)
			{
				hasMatch = true;
				break;
			}
		}
		assert(hasMatch, "Sent data is not equal to received data.");
	}
}