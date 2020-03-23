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
 * Set a socket's send and receive timeouts to some random value in the range [150, 300] ms.
 *
 * Params:
 * 	conn = The socket whose timeouts are being set.
 *
 * Throws: SocketException on failure.
 */
static void _setRandomTimeout(Socket conn)
{
	Duration timeout = dur!"msecs"(150 + uniform!"[]"(0, 150));
	conn.setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, timeout);
	conn.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, timeout);
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
	private shared _QualifiedEntry[] _log;
	
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
		Entry!T payload;
		
		this(Entry!T pl)
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
			
			Duration timeout = dur!"msecs"(150 + uniform!"[]"(0, 150));
			synchronized(_paxosLock)
			{
				//Wait for a heartbeat.
				bool heartbeat = false;
				MonoTime start = MonoTime.currTime, now = start;
				while(!heartbeat && now - start < timeout)
				{
					heartbeat = (cast(Condition)_heartbeat).wait(timeout - (now - start));	//Note: This yields _paxosLock.
					now = MonoTime.currTime;
				}
				
				//Try to become leader if no heartbeat arrived.
				if(!heartbeat)
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
										
										_sender.perform(_config[idx],
											delegate bool(Socket conn)
											{
												_setRandomTimeout(conn);
												_RPC identifier = _RPC.Vote;
												if(sendType(conn, identifier))
												{
													if(sendType(conn, candidateTerm))
													{
														ID voterTerm;
														if(receiveType(conn, voterTerm))
														{
															ballot[0] = true;
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
	 *
	 * Params:
	 * 	term = The ID identifying the term in which this node is leading.
	 */
	private void _lead(ID term)	//Should this just assume that _paxosLock is held?
	{
		while(true)
		{
			synchronized(_closeLock)
			{
				if(_closing)
				{
					break;
				}
			}
			
			//TODO: send prepare/accept/success RPCs. 
			//send out heartbeats every 50(?) ms
			//could heartbeats be (potentially empty) success rpcs?
			//refer to proposal numbers on incoming RPC to know when to give up leadership
			writeln("(", _self, ") Leading in (", term.round, ", ", term.node, ")");
			import core.thread : Thread;
			Thread.sleep(dur!"msecs"(100));
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
				_setRandomTimeout(conn);	//Don't bother resetting this, it'll just get set again in the future.
				
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
						//(follower -> leader) (prop, value): add value to the next available spot [resp: (?)]
						return true;
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
	 * 	conn = The socket on which to communicate with the candidate.
	 *
	 * Returns: True if the vote completed successfully, false otherwise.
	 */
	private bool _vote(Socket conn)
	{
		bool voted = false;
		scope(exit)
		{
			if(voted)
			{
				spawn(
					() shared
					{
						synchronized(_paxosLock)
						{
							(cast(Condition)_heartbeat).notify();
						}
					}
				);
			}
		}
		
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
					voted = true;
					writeln("\t\t(", _self, ") Voted for (", _term.round, ", ", _term.node, ")");
				}
				
				return success;
			}
		}
		
		return false;
	}
}
unittest
{
	Configuration cfg = [new InternetAddress("localhost", 32320), new InternetAddress("localhost", 32321)];
	
	shared Consensus!InternetAddress cons0 = new shared Consensus!InternetAddress(0, cfg, "localhost", cast(ushort)1);
	scope(exit)
	{
		import core.thread : Thread;
		Thread.sleep(dur!"seconds"(1));
		cons0.teardown();
	}
	
	shared Consensus!InternetAddress cons1 = new shared Consensus!InternetAddress(1, cfg, "localhost", cast(ushort)1);
	scope(exit) cons1.teardown();
	
	import core.thread : Thread;
	Thread.sleep(dur!"seconds"(2));
}

import std.stdio : writeln;	//temporary