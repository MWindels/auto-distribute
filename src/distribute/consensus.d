/**
 * Provides an implementation of multi-paxos used to distribute arbitrary types.
 *
 * Author: Michael Windels.
 */
module distribute.consensus;

import std.socket;
import std.typecons : Rebindable, Nullable;
import std.concurrency;
import std.random;
//import distribute.entry;
import distribute.id;

package:

/**
 * Contains multi-paxos consensus state/logic.
 */
class Consensus(T)
{
	private immutable Tid _manager;
	
	private immutable ushort _listenPort;
	private immutable Tid _listener;
	
	private shared Object _closeLock;
	private shared bool _closing = false;
	
	private shared Object _paxosLock;
	private shared Nullable!Tid _follower;	//The thread responsable for interfacing with the leader node.
	private shared ID _proposal;	//The highest proposal value seen so far.
	
	private enum _Operation : ubyte {_Prepare, _Accept, _Success, _Heartbeat}
	
	/**
	 * Starts all of the necessary threads for a Consensus object.
	 *
	 * Params:
	 * 	listenPort = The port by which vote requests arrive.
	 * 	args = The arguments supplied to the data structure being distributed (if it's type is initialized with  a constructor, otherwise just an initial value).
	 */
	this(Args...)(ushort listenPort, Args args)
	{
		_manager = spawn(&_manage, cast(shared)_initData(args));
		
		_listenPort = listenPort;
		_listener = spawn(&_listen);
	}
	
	/**
	 * Stops all of the threads initiated by the constructor.
	 */
	~this()
	{
		synchronized(_closeLock)
		{
			_closing = true;
		}
		_nopCandidate();
	}
	
	/**
	 * Initializes a copy of the data structure being distributed.
	 *
	 * Params:
	 * 	args = The arguments supplied to the data structure's constructor, or an initial value if the type isn't initialized with a constructor.
	 *
	 * Returns: A new object of type T constructed with (or equal to) args.
	 */
	private T _initData(Args...)(Args args) const
	if(is(T == class) || is(T == struct) || is(T == union) || (Args.length == 1 && is(Args[0] == T)))
	{
		static if(is(T == class))
		{
			return new T(args);
		}
		else static if(is(T == struct) || is(T == union))
		{
			return T(args);
		}
		else
		{
			return args[0];
		}
	}
	
	/**
	 * Controls access to the data structure being distributed and applies consensus logic.
	 *
	 * Params:
	 * 	sharedData = The data structure being distributed.
	 */
	private void _manage(shared T sharedData)
	{
		T data = cast(T)sharedData;
		
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			_Operation op;
			bool recv = receiveTimeout(
				dur!"msecs"(150 + uniform!"[]"(0, 150)),
				(_Operation o) {op = o;}
			);
			
			if(recv)
			{
				switch(op)
				{
					case _Operation._Prepare:
						//prepare
						break;
					case _Operation._Accept:
						//accept
						break;
					case _Operation._Success:
						//success
						break;
				}
			}
			else
			{
				//elect self as new leader.
			}
		}
	}
	
	/**
	 * Listens for leader candidates attempting to connect with this node.
	 */
	private void _listen()
	{
		TcpSocket listener = new TcpSocket(AddressFamily.INET);
		listener.bind(new InternetAddress("localhost", _listenPort));
		scope(exit) listener.close();
		
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			spawn(&_vote, cast(shared)listener.accept());
		}
	}
	
	/**
	 * Spoofs a candidate connection.
	 * Used to terminate _listener.
	 */
	private void _nopCandidate()
	{
		TcpSocket listener = new TcpSocket(AddressFamily.INET);
		
		try
		{
			listener.connect(new InternetAddress("localhost", _listenPort));
		}
		catch(SocketOSException)
		{
			return;
		}
		
		scope(exit) listener.close();
		listener.send(cast(ubyte[0])[]);	//This zero-length packet will automatically be rejected by _vote.
	}
	
	/** 
	 * Casts a vote for a candidate.
	 * If the candidate's proposal number is higher than the local number, vote for them.
	 * If the candidate's proposal number is equal to the local number, the connection was broken and is being re-established.
	 *
	 * Params:
	 * 	sharedCandidate = A TcpSocket for communicating with the candidate.
	 */
	private void _vote(shared TcpSocket sharedCandidate)
	{
		TcpSocket candidate = cast(TcpSocket)sharedCandidate;	//Assumes that the owner thread will not use sharedCandidate.
		scope(exit) candidate.close();
		
		ID candidateProposal;
		ptrdiff_t recvSize = 0;
		try
		{
			recvSize = candidate.receive((cast(ubyte*)&candidateProposal)[0 .. candidateProposal.sizeof]);
		}
		catch(SocketOSException)
		{
			return;
		}
		
		if(recvSize == candidateProposal.sizeof)
		{
			ID tentativeProposal;
			synchronized(_paxosLock)
			{
				tentativeProposal = _proposal;
			}
			
			try
			{
				candidate.send((cast(ubyte*)&tentativeProposal)[0 .. tentativeProposal.sizeof]);	//Cast tentative vote.
			}
			catch(SocketOSException)
			{
				return;
			}
			
			//If candidate's proposal is greater, then they're a new leader.
			//If candidate's proposal is equal, then they're re-establishing a dropped connection.
			if(tentativeProposal <= candidateProposal)
			{
				synchronized(_paxosLock)
				{
					if(_proposal == tentativeProposal)
					{
						/*if(!_follower.isNull)
						{
							send!ubyte(_follower.get(), 0);
						}
						
						_follower = thisTid;*/
						_proposal = candidateProposal;
					}
					else
					{
						return;
					}
				}
				
				_follow(candidate);
			}
		}
	}
	
	/**
	 *
	 */
	private void _follow(TcpSocket candidate)
	{
		//if recv'd a ubyte, nullify _follower
	}
}