/**
 * Provides an implementation of multi-paxos used to distribute arbitrary types.
 *
 * Author: Michael Windels.
 */
module distribute.consensus;

import std.socket;
import std.concurrency;
import core.sync.mutex;
import core.sync.condition;
import core.time : dur, Duration, MonoTime;
import std.random : uniform;
import std.typecons : Tuple;
import distribute.utils.thread;
import distribute.pool;
import distribute.id;
import distribute.entry;

private:

/**
 * Generates a random timeout in the range [low, low + var] ms.
 *
 * Params:
 * 	low = The lower bound of the timeout.
 * 	var = When added to low, this represents the upper bound of the timeout.
 *
 * Returns: A random timeout in the range [low, low + var] ms.
 */
static Duration _randomTimeout(uint low, uint var)
{
	return dur!"msecs"(low + uniform!"[]"(0, var));
}

/**
 * Set a socket's send and receive timeouts to some specified value.
 *
 * Params:
 * 	conn = The socket whose timeouts are being set.
 * 	timeout = The duration of the timeout.
 *
 * Throws: SocketException on failure.
 */
static void _setTimeout(Socket conn, Duration timeout)
{
	conn.setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, timeout);
	conn.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, timeout);
}

/**
 * Waits on a condition variable for some time.
 * Assumes that cvar's lock is held.
 * Handles spurious wakeups.
 *
 * Params:
 * 	cvar = The condition variable to wait on.
 * 	timeout = The duration of time to wait for.
 *
 * Returns: True if the cvar was notified before the timeout, otherwise false.
 */
static bool _wait(ref shared Condition cvar, Duration timeout)
{
	bool notified = false;
	MonoTime start = MonoTime.currTime, now = start;
	while(!notified && now - start < timeout)
	{
		notified = (cast(Condition)cvar).wait(timeout - (now - start));	//Yields the lock held by cvar.
		now = MonoTime.currTime;
	}
	
	return notified;
}


package:

/**
 * Holds the addresses of all the nodes in a system.
 */
alias Configuration = InternetAddress[];

/**
 * Contains multi-paxos consensus state/logic.
 */
shared class Consensus(T)
{
	//Internal threads.
	private immutable Tid _manager;
	private immutable Tid _elector;
	
	//System configuration data.
	private immutable uint _self;
	private immutable Configuration _config;
	
	//Send/Receive utilities.
	private shared ConnectionPool _sender;
	private shared TerminalPool _receiver;
	
	//Paxos state.
	private shared Mutex _paxosLock;
	private shared Condition _heartbeat;
	private shared ID _term;
	private shared bool _leading = false;	//Necessary as there are times when _term.node == _self but the node is not leading.
	private shared _QualifiedEntry[] _log;
	
	//Request state.
	private shared Object _requestsLock;
	private shared uint _requests = 0;
	
	//Teardown data.
	private shared Object _closeLock;
	private shared bool _closing = false;
	
	//Internal types.
	private enum _RPC : ubyte {Vote, Prepare, Accept, Success, Request}
	private struct _QualifiedEntry
	{
		//Object lock;	//Individual log entries can be worked on concurrently, but we need to ensure that the log is stable on resize.
		//proposal number
		//is-used flag
		//is-chosen flag
		immutable Entry!T payload;
		
		this(immutable Entry!T pl)
		{
			//lock = new Object();
			payload = pl;
		}
	}
	
	/**
	 * Creates a new Consensus object.
	 *
	 * Params:
	 * 	id = This node's identification number.
	 * 	cfg = The system configuration to use.
	 * 	args = The arguments supplied to the data structure being distributed (if it's type is initialized with a constructor, otherwise just an initial value).
	 */
	this(Args...)(uint id, const Configuration cfg, Args args) shared	//The types of Args cannot be inferred when this function lacks an explicit shared qualifier.
	{
		assert(id < cfg.length, "ID does not correspond to a node.");
		
		_self = id;
		_config = cast(immutable)cfg;
		
		_sender = new shared ConnectionPool(100, 60);
		_receiver = new shared TerminalPool(_config[_self].port, _config.length, 50, 120, cast(shared)&_demux);
		
		_paxosLock = cast(shared)(new Mutex());
		_heartbeat = cast(shared)(new Condition(cast(Mutex)_paxosLock));
		_term = _proposal(0);
		
		_requestsLock = new Object();
		
		_closeLock = new Object();
		
		_manager = cast(immutable)spawn(&_manage, cast(shared)_initData(args));
		_elector = cast(immutable)spawn(&_elect);
	}
	
	/**
	 * Closes all threads spawned by a Consensus object.
	 */
	public void teardown()
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
		
		waitForTids(_manager, _elector);
		_receiver.close();
		_sender.close();
	}
	
	/**
	 * Helper function that generates a unique proposal number for this particular node.
	 *
	 * Params:
	 * 	round = The new proposal number's round number.
	 *
	 * Returns: A proposal number containing (round, _self).
	 */
	private ID _proposal(uint round) const
	{
		return ID(round, _self);
	}
	
	/**
	 * Helper function that initializes a copy of the data structure being distributed.
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
	 * Controls access to the data structure being distributed.
	 *
	 * Params:
	 * 	sharedData = The data structure being distributed (although it is shared, it is assumed to be inaccessible from other threads).
	 */
	private void _manage(shared T sharedData)
	{
		scope(exit) send(ownerTid, thisTid);
		
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
			
			//TODO: apply log entries
			import core.thread : Thread;
			Thread.sleep(dur!"msecs"(50));
		}
	}
	
	/**
	 * Controls when this node elects itself leader.
	 */
	private void _elect()
	{
		scope(exit) send(ownerTid, thisTid);
		
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			synchronized(_paxosLock)
			{
				//Wait for a heartbeat.
				//If none arrive, try to become leader.
				if(!_wait(_heartbeat, _randomTimeout(150, 150)))
				{
					//Set up for a new election.
					_term = _proposal(_term.round + 1);
					
					//Request a vote from each node.
					for(int i = 0; i < _config.length; ++i)
					{
						if(i != _self)
						{
							writeln("(", _self, ") Vote sent to addr: (", i, ", ", _config[i].port, ") in (", _term.round, ", ", _term.node, ")");
							spawn(
								(ID candidateTerm, int idx) shared
								{
									try
									{
										Tuple!(bool, ID) ballot = Tuple!(bool, ID)(false, ID(0, 0));
										scope(exit) send(ownerTid, ballot);
										
										ballot[0] = _sender.perform(_config[idx],
											delegate bool(Socket conn)
											{
												_setTimeout(conn, _randomTimeout(150, 150));
												
												_RPC identifier = _RPC.Vote;
												if(sendType(conn, identifier))
												{
													if(sendType(conn, candidateTerm))
													{
														ID voterTerm;
														if(receiveType(conn, voterTerm))
														{
															ballot[1] = voterTerm;
															writeln("(", _self, ") Recv'd ballot (", voterTerm.round, ", ", voterTerm.node, ") in (", candidateTerm.round, ", ", candidateTerm.node, ")");
															return true;
														}
													}
												}
												
												return false;
											}
										);
									}
									catch(SocketException) {}
								},
								_term, i
							);
						}
					}
					
					//Wait to hear from a majority of nodes.
					ID highestTerm = _term;
					uint votes = 1, ballots = 1;
					while(ballots < _config.length)
					{
						receive(
							(Tuple!(bool, ID) ballot)
							{
								ballots += 1;
								if(ballot[0])
								{
									if(ballot[1] < highestTerm)
									{
										votes += 1;
									}
									else
									{
										highestTerm = ballot[1];
									}
								}
							}
						);
					}
					
					writeln("(", _self, ") Got ", votes, "/", ballots, " votes in (", _term.round, ", ", _term.node, ")");
					
					//If another node sent a higher term, abort.
					if(cast(ID)_term < highestTerm)
					{
						//persist new value of _term!
						_term = highestTerm;
						continue;
					}
					
					//Lead if a majority of votes were received.
					if(votes > cast(double)_config.length / 2.0)
					{
						_lead(_term);
					}
				}
			}
		}
	}
	
	/**
	 * Controls what the node does while leading.
	 * Assumes that _paxosLock is held.
	 *
	 * Params:
	 * 	term = The ID identifying the term in which this node is leading.
	 */
	private void _lead(ID term)
	{
		_leading = true;
		scope(exit) _leading = false;
		
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			//If leadership has lapsed, resign.
			if(term < _term)
			{
				return;
			}
			
			//TODO: send prepare/accept/success RPCs. 
			//send out heartbeats every 50(?) ms
			//could heartbeats be (potentially empty) success rpcs?
			//refer to proposal numbers on incoming RPC to know when to give up leadership
			writeln("(", _self, ") Leading in (", term.round, ", ", term.node, ")");
			{
				_paxosLock.unlock();
				scope(exit) _paxosLock.lock();
				
				import core.thread : Thread;
				Thread.sleep(dur!"msecs"(100));
			}
		}
		writeln("(", _self, ") Not Leading!");
	}
	
	/**
	 * Handles an incoming message.
	 *
	 * Params:
	 * 	conn = The socket on which data is arriving.
	 *
	 * Returns: True if the operation encoded in the message completed successfully, false otherwise.
	 */
	private bool _demux(Socket conn)
	{
		try
		{
			_RPC identifier;
			if(receiveType(conn, identifier))
			{
				_setTimeout(conn, _randomTimeout(150, 150));	//Don't bother resetting this, it'll just get set again in the future.
				
				switch(identifier)
				{
					case _RPC.Vote:
						return _vote(conn);	//(leader -> follower) (prop): elect me as leader [resp: (prop)]
					case _RPC.Prepare:
						//(leader -> follower) (prop, idx): tell me about value in idx for a future accept [resp: (value if any, next unaccepted idx OR none), if none then prepares no longer necessary]
						return true;
					case _RPC.Accept:
						//(leader -> follower) (prop, idx, value): assign value to spot idx [resp: (?)]
						return true;
					case _RPC.Success:
						//(leader -> follower) (prop, idx, value): entry at idx is chosen as value [resp: (?)]
						return true;
					case _RPC.Request:
						return _request(conn);	//(follower -> leader) (prop, value): add value to the next available spot [resp: (?)]
					default:
						return false;
				}
			}
		}
		catch(SocketException) {}	//Assumes that the case-handling functions left the Consensus object in a valid state.
		
		return false;
	}
	
	/**
	 * Casts a vote for a leader if their term number is higher than the local node's number.
	 *
	 * Params:
	 * 	conn = The socket by which to communicate with the candidate.
	 *
	 * Returns: True if communication completed successfully, false otherwise.
	 */
	private bool _vote(Socket conn)
	{
		writeln("\t(", _self, ") Voting...");
		
		ID candidateTerm;
		if(receiveType(conn, candidateTerm))
		{
			synchronized(_paxosLock)
			{
				bool success = sendType(conn, _term);
				writeln("\t(", _self, ") Sent ballot (", _term.round, ", ", _term.node, ") in response to (", candidateTerm.round, ", ", candidateTerm.node, ")");	//This frequently gets called twice for some reason.
				if(cast(ID)_term < candidateTerm)
				{
					_term = candidateTerm;
					(cast(Condition)_heartbeat).notifyAll();
					writeln("\t\t(", _self, ") Voted for (", _term.round, ", ", _term.node, ")");
				}
				
				return success;
			}
		}
		
		return false;
	}
	
	/**
	 * If this node is the leader, it adds a new entry to the log.
	 *
	 * Params:
	 * 	conn = The socket by which to communicate with the requester.
	 *
	 * Returns: True if communication completed successfully, false otherwise.
	 */
	private bool _request(Socket conn)
	{
		//The entry is received first so we spend less time holding the lock.
		Tuple!(ID, Entry!T) identifiedEntry;
		if(receiveType(conn, identifiedEntry))
		{
			synchronized(_paxosLock)
			{
				if(sendType(conn, _leading))
				{
					if(_leading)
					{
						//add the entry to the log!
						//if successful, update the highest recv'd request for the node, then send results back
						Results!T res = Results!T();	//temporary
						return sendType(conn, res);
					}
					else
					{
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Initiate a request to add an entry to the log.
	 *
	 * Params:
	 * 	entry = The log entry to add.
	 *
	 * Returns: The results of applying the entry.
	 */
	public Results!T request(const Entry!T entry)
	{
		uint req;
		synchronized(_requestsLock)
		{
			req = _requests;
			_requests = _requests + 1;
		}
		
		Results!T results;
		bool success = false;
		while(!success)
		{
			uint leader;
			synchronized(_paxosLock)
			{
				leader = _term.node;	//Might be prudent to sleep for a few ms (w/o lock) if leader == _term.node already (indicates past failure and no change).
				
				//This implies that leader == _self (but the converse is not necessarily true).
				if(_leading)
				{
					//handle request locally by sending out accept messages while the lock is held
				}
			}
			
			//If this node is not leader, try another node.
			if(leader != _self)
			{
				try
				{
					//If another node is leading, send them a request.
					//Note that &= is used to set success to true iff the delegate set success to true and perform returned true.
					success &= _sender.perform(_config[leader],
						delegate bool(Socket conn)
						{
							_setTimeout(conn, _randomTimeout(150, 150));
							
							_RPC identifier = _RPC.Request;
							if(sendType(conn, identifier))
							{
								Tuple!(ID, const Entry!T) identifiedEntry = Tuple!(ID, const Entry!T)(ID(req, _self), entry);	//Use Cerealed to serialize the entry in the future.
								if(sendType(conn, identifiedEntry))
								{
									bool leading;
									if(receiveType(conn, leading))
									{
										if(leading)
										{
											conn.blocking(false);
											scope(exit) conn.blocking(true);
											
											synchronized(_paxosLock)
											{
												//While awaiting a result, ensure that heartbeats are arriving.
												while(!receiveType(conn, results))	//might make sense to set an upper bound on the number of recvs that can happen (so it doesn't loop forever)
												{
													//If the leader failed to send a heartbeat, consider the connection toast.
													//If leadership changed, also close non-gracefully (it would be too much hassle to gracefully shut down).
													if(!_wait(_heartbeat, _randomTimeout(150, 150)) || _term.node != leader)
													{
														return false;
													}
												}
											}
											
											success = true;
											return true;
										}
										else
										{
											return true;
										}
									}
								}
							}
							
							return false;
						}
					);
				}
				catch(SocketException) {}
			}
		}
		
		return results;
	}
}
unittest
{
	import std.container.slist;
	alias TestType = SList!int;
	Configuration cfg = [new InternetAddress("localhost", 32320), new InternetAddress("localhost", 32321)];
	
	shared Consensus!TestType cons0 = new shared Consensus!TestType(0, cfg, 1, 2, 3);
	scope(exit) cons0.teardown();
	
	shared Consensus!TestType cons1 = new shared Consensus!TestType(1, cfg, 1, 2, 3);
	scope(exit) cons1.teardown();
	
	import core.thread : Thread;
	Thread.sleep(dur!"seconds"(2));
	
	string[] loc = [];	//Would normally be supplied by Distributed!T.
	cons1.request(Entry!TestType().__ctor!(TestType.removeAny)(loc));	//The removeAny function is nice because it has no template parameters (which entries cannot handle).
	writeln("(1) Submitted Request!!!");
}

import std.stdio : writeln;	//temporary