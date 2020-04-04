/**
 * Defines a thread-safe Socket Pool type.
 * Note: Some of the array operations (appends and slicings mainly) can probably be replaced with less expensive operations.
 *
 * Author: Michael Windels.
 */
module distribute.pool;

import std.socket;
import std.concurrency;
import core.time : dur, MonoTime;
import std.algorithm.iteration : map;
import std.typecons : Tuple;
import std.array : array;
import core.thread : Thread;
import distribute.utils.thread;

private:

/**
 * A type which contains a (shared) socket and a timestamp.
 */
struct _StampedSocket
{
	shared Socket socket;
	MonoTime timestamp;
}

/**
 * Removes the values from an array specified by a sorted array of indices.
 *
 * Params:
 * 	array = The array from which to remove elements.
 * 	indices = The sorted array of indices to remove.
 *
 * Returns: The "array" array without the elements at the indices specified by "indices".
 */
static T[] _removeIndices(T)(T[] array, const uint[] indices)
{
	for(int i = 0, j = 0; i < array.length - indices.length;)
	{
		if(j < indices.length && i + j == indices[j])
		{
			j += 1;
		}
		else
		{
			if(j > 0)
			{
				array[i] = array[i + j];
			}
			i += 1;
		}
	}
	return array[0 .. ($ - indices.length)];
}


package:

/**
 * Sends an instance of some type to a socket.
 * Intended for non-reference and non-pointer types (and types that do not contain references or pointers).
 *
 * Params:
 * 	conn = The socket to send to.
 * 	value = The value to send.
 *
 * Returns: True if value.sizeof bytes were sent, otherwise false.
 *
 * Throws: Anything thrown by Socket.send().
 */
static bool sendType(T)(Socket conn, in ref T value)
{
	return conn.send((cast(const void*)&value)[0 .. value.sizeof]) == value.sizeof;
}

/**
 * Receives an instance of some type from a socket.
 * Intended for non-reference and non-pointer types (and types that do not contain references or pointers).
 *
 * Params:
 * 	conn = The socket to receive from.
 * 	value = The value received (return parameter).
 *
 * Returns: True if value.sizeof bytes were received, otherwise false.
 *
 * Throws: Anything thrown by Socket.receive().
 */
static bool receiveType(T)(Socket conn, out T value)
{
	ubyte[value.sizeof] buffer;
	bool success = conn.receive(buffer) == value.sizeof;
	value = *(cast(T*)buffer);
	return success;
}

/**
 * A pool of re-usable TCP sockets.
 * This type is used to initiate connections.
 */
shared class ConnectionPool
{
	private immutable Tid _culler;
	
	private shared Object _poolLock;
	private shared bool _poolActive = true;
	private shared (_StampedSocket[])[_HashableAddress] _pool;
	
	private shared Object _closeLock;
	private shared bool _closing = false;
	
	private alias _HashableAddress = Tuple!(typeof(InternetAddress.addr), typeof(InternetAddress.port));
	
	/**
	 * Creates a new ConnectionPool.
	 *
	 * Params:
	 * 	interv = The number of milliseconds in a culling interval (informs the upper bound on idleThresh).
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 */
	this(uint interv, uint idleThresh)
	{
		_poolLock = new Object();
		_closeLock = new Object();
		_culler = cast(immutable)spawn(&_cull, interv, idleThresh);
	}
	
	/**
	 * Closes a ConnectionPool and all threads spawned by it.
	 * See close() in TerminalPool for details.
	 */
	public void close()
	{
		synchronized(_closeLock)
		{
			if(_closing)
			{
				return;
			}
			else
			{
				_closing = true;
			}
		}
		
		waitForTids(_culler);
	}
	
	/**
	 * Removes old connections from the connection pool.
	 *
	 * Params:
	 * 	interv = The number of milliseconds in a culling interval (informs the upper bound on idleThresh).
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 */
	private void _cull(uint interv, uint idleThresh)
	{
		scope(exit) send(ownerTid, thisTid);
		
		scope(exit)
		{
			//Close all connections in the pool before leaving this function.
			synchronized(_poolLock)
			{
				_poolActive = false;
				foreach(conns; _pool)
				{
					foreach(conn; conns)
					{
						(cast(Socket)conn.socket).close();
					}
				}
			}
		}
		
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			//Remove connections from the pool that have been idle for too long.
			//Note that every connection array is sorted by timestamp.
			synchronized(_poolLock)
			{
				MonoTime closingTime = MonoTime.currTime;
				foreach(addr, conns; _pool)
				{
					uint closedConns = 0;
					for(; closedConns < conns.length; ++closedConns)
					{
						if(closingTime - conns[closedConns].timestamp >= dur!"seconds"(idleThresh))
						{
							(cast(Socket)conns[closedConns].socket).close();
						}
						else
						{
							break;
						}
					}
					if(closedConns > 0)
					{
						_pool[addr] = conns[closedConns .. $];
					}
				}
			}
			
			//Wait before re-checking the connections.
			Thread.sleep(dur!"msecs"(interv));
		}
	}
	
	/**
	 * Grabs a connection and passes it to a delegate so it can use that open connection.
	 * The delegate must return a boolean denoting whether or not it successfully completed communication.
	 * Furthermore, the delegate must not close the connection.
	 * Can be called from multiple threads simultaneously.
	 *
	 * Params:
	 * 	addr = The address to connect to.
	 * 	dl = A delegate which operates on an open connection.
	 *
	 * Returns: True if dl completed successfully, false otherwise.
	 */
	public bool perform(const InternetAddress addr, bool delegate(Socket) dl)
	{
		shared Socket conn;
		bool newConn = true;
		_HashableAddress key = _HashableAddress(addr.addr, addr.port);
		
		synchronized(_poolLock)
		{
			if(!_poolActive)
			{
				return false;
			}
			
			shared(_StampedSocket[])* conns = key in _pool;
			if(conns == null)
			{
				_pool[key] = cast(shared _StampedSocket[])[];
				conns = key in _pool;
			}
			else if(conns.length > 0)
			{
				conn = (*conns)[0].socket;
				*conns = (*conns)[1 .. $];	//Calling _removeIndices would be wasteful.
				newConn = false;
			}
		}
		
		if(newConn)
		{
			conn = cast(shared)(new Socket(AddressFamily.INET, SocketType.STREAM, ProtocolType.TCP));
			(cast(Socket)conn).connect(new InternetAddress(addr.addr, addr.port));	//Might be best to just throw an exception for the user to deal with (they provided the address after all).
		}
		
		scope(failure)
		{
			(cast(Socket)conn).close();
		}
		
		if(dl(cast(Socket)conn))
		{
			synchronized(_poolLock)
			{
				if(_poolActive)
				{
					_pool[key] ~= [_StampedSocket(conn, MonoTime.currTime)];
				}
				else
				{
					(cast(Socket)conn).close();
				}
			}
			
			return true;
		}
		else
		{
			(cast(Socket)conn).close();
			return false;
		}
	}
}
unittest
{
	import std.random : uniform;
	
	immutable uint num = 10;
	immutable uint size = 16;
	immutable Tid mainThread = cast(immutable)thisTid;
	
	shared Object successesLock = new Object();
	shared bool[num] successes;	//All elements default initialized to false.
	bool delegate(Socket) process = (Socket conn)
	{
		Thread.sleep(dur!"msecs"(uniform!"[]"(0, 1000)));
		
		ubyte[size] recvBuffer;
		assert(conn.receive(recvBuffer) == size, "Received too many/few bytes.");
		
		if(recvBuffer[0] < num)
		{
			synchronized(successesLock)
			{
				assert(!successes[recvBuffer[0]], "Received already-received starting byte.");
				successes[recvBuffer[0]] = true;
			}
			
			for(int j = 1; j < size; ++j)
			{
				assert((recvBuffer[j - 1] + 1) % size == recvBuffer[j], "Received unexpected subsequent byte.");
			}
		}
		else
		{
			assert(false, "First received byte was beyond expected bounds.");
		}
		
		send!ubyte(cast(Tid)mainThread, 0);
		return true;	//Because of the asserts there's no reason to return false.
	};
	
	//These tests use a TerminalPool rather than an accept() socket because the ConnectionPool may create less than <num> sockets (and simply re-use them).
	shared TerminalPool term1 = new shared TerminalPool(32856, num, 50, 2, process);
	scope(exit) term1.close();
	shared TerminalPool term2 = new shared TerminalPool(32858, num, 50, 2, process);
	scope(exit) term2.close();
	
	//I figure idleThresh should be lower on the connection side so the terminal side is less likely to close the corresponding socket before the connection side has a chance to send anything.
	shared ConnectionPool pool = new shared ConnectionPool(200, 1);
	scope(exit) pool.close();
	
	for(int i = 0; i < num; ++i)
	{
		spawn(
			(int idx) shared
			{
				auto sender = delegate bool(Socket conn)
				{
					ubyte[size] sendBuffer;
					for(int j = 0; j < size; ++j)
					{
						sendBuffer[j] = (idx + j) % size;
					}
					conn.send(sendBuffer);
					return true;	//No real failure state, later asserts handle that.
				};
				
				Thread.sleep(dur!"msecs"(uniform!"[]"(0, 1000)));
				if(idx % 2 == 0)
				{
					assert(pool.perform(new InternetAddress("localhost", 32856), sender), "ConnectionPool failed to perform().");
				}
				else
				{
					assert(pool.perform(new InternetAddress("localhost", 32858), sender), "ConnectionPool failed to perform().");
				}
				
			},
			i
		);
	}
	
	for(int i = 0; i < num; ++i)
	{
		assert(receiveTimeout(dur!"seconds"(15), (ubyte) {}), "At least one connection was not made.");
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
	 * Also note that process must return a boolean denoting whether or not it successfully completed communication.
	 * Furthermore, process must not close the Socket passed to it.
	 *
	 * Params:
	 * 	port = The port on which to listen for connections.
	 * 	conns = The highest number of concurrent connections expected.
	 * 	interv = The number of milliseconds in a select interval (informs the upper bound on idleThresh).
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 * 	process = The delegate which handles data received from an open connection (must be thread-safe).
	 */
	this(ushort port, uint conns, uint interv, uint idleThresh, shared bool delegate(Socket) process)
	{
		_closeLock = new Object();
		_listener = cast(immutable)spawn(&_listen, port, conns, interv, idleThresh, process);
	}
	
	/**
	 * Closes a TerminalPool and all threads spawned by it.
	 * The user must call this before the object is destroyed.
	 * Because this allocates memory, it cannot be called in the destructor.
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
		
		waitForTids(_listener);
	}
	
	/**
	 * Listens for incoming connections and processes them.
	 *
	 * Params:
	 * 	port = The port on which to listen for connections.
	 * 	conns = The highest number of concurrent connections expected.
	 * 	interv = The number of milliseconds in a select interval (informs the upper bound on idleThresh).
	 * 	idleThresh = The lower bound on the length of time (in seconds) which a connection can spend idle (unused) before it is closed.
	 * 	process = The delegate which handles data received from an open connection (must be thread-safe).
	 */
	private void _listen(ushort port, uint conns, uint interv, uint idleThresh, shared bool delegate(Socket) process) shared
	{
		scope(exit) send(ownerTid, thisTid);
		
		//Build a connection-accepting socket.
		Socket listener = new Socket(AddressFamily.INET, SocketType.STREAM, ProtocolType.TCP);
		listener.bind(new InternetAddress("localhost", port));
		listener.listen(conns);
		scope(exit) listener.close();
		
		//Keep track of which sockets are free.
		//Note: _StampedSocket stores shared Sockets, but they're only ever accessed from one thread while they're in the free array (hence why shared is cast away so often).
		SocketSet readable = new SocketSet();
		_StampedSocket[] free = [];
		
		//Keep track of which sockets were recently used.
		shared Object recentLock = new Object();
		shared Socket[] recent = [];
		
		//Keep track of how many sockets are busy.
		shared Object busyLock = new Object();
		shared uint busy = 0;
		
		//Keep track of whether this thread is terminating or not.
		shared Object exitLock = new Object();
		shared bool exiting = false;
		
		//Wrap the process delegate with some house-keeping code.
		void delegate(shared Socket) shared wrappedProcess = (shared Socket conn) shared
		{
			scope(exit)
			{
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
			}
			
			scope(failure)
			{
				(cast(Socket)conn).close();
			}
			
			if(process(cast(Socket)conn))
			{
				synchronized(recentLock)
				{
					recent ~= [conn];
				}
			}
			else
			{
				(cast(Socket)conn).close();
			}
		};
		
		//On exit, close all open connections.
		scope(exit)
		{
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
				(cast(Socket)conn.socket).close();
			}
			synchronized(recentLock)
			{
				foreach(conn; recent)
				{
					(cast(Socket)conn).close();
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
			
			//Close connections that have been inactive for too long.
			//Note that elements in the free array are sorted by their timestamps.
			uint closedConns = 0;
			MonoTime closingTime = MonoTime.currTime;
			for(; closedConns < free.length; ++closedConns)
			{
				if(closingTime - free[closedConns].timestamp >= dur!"seconds"(idleThresh))
				{
					(cast(Socket)free[closedConns].socket).close();
				}
				else
				{
					break;
				}
			}
			if(closedConns > 0)
			{
				free = free[closedConns .. $];
			}
			
			//Move all recently-freed sockets back to the free pool.
			synchronized(recentLock)
			{
				MonoTime freeingTime = MonoTime.currTime;
				free ~= map!(
					(shared Socket conn)
					{
						return _StampedSocket(conn, freeingTime);
					}
				)(recent).array;	//Using ~= to append an array() isn't the most efficient way to append a large Range, but recent is likely small anyways.
				recent = [];
			}
			
			//Re-build the set of sockets to be read from.
			readable.reset();	//Faster than calling isset() for all members?
			readable.add(listener);
			foreach(conn; free)
			{
				readable.add(cast(Socket)conn.socket);
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
					
					shared Socket newConn = cast(shared)listener.accept();	//Catch SocketAcceptException?
					spawn(wrappedProcess, newConn);
				}
				
				uint[] activeIdxes = [];
				foreach(idx, conn; free)
				{
					//It's kind of ridiculous that we can't iterate over a SocketSet.  We have the means, it's not like this is C!
					if(readable.isSet(cast(Socket)conn.socket) != 0)
					{
						synchronized(busyLock)
						{
							busy = busy + 1;
						}
						
						activeIdxes ~= [idx];
						spawn(wrappedProcess, conn.socket);
					}
				}
				free = _removeIndices(free, activeIdxes);
			}
		}
	}
}
unittest
{
	import std.random : uniform;
	
	immutable uint num = 10;
	immutable uint size = 16;	//If this is lower than num, then we'll have duplicate sendBuffers.
	immutable Tid mainThread = cast(immutable)thisTid;
	
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
	shared (bool delegate(Socket)) recv =
	(Socket conn)
	{
		shared ubyte[size] interimBuffer;
		Thread.sleep(dur!"msecs"(uniform!"[]"(0, 2000)));
		conn.receive(cast(ubyte[size])interimBuffer);
		
		synchronized(recvLock)
		{
			recvBuffers ~= interimBuffer;
		}
		
		send!ubyte(cast(Tid)mainThread, 0);
		return true;	//Implies no invalid state.
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
		Thread.sleep(dur!"msecs"(uniform!"[]"(0, 2000)));
		
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