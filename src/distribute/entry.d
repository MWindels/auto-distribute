/**
 * Contains definitions for multi-paxos log entries.
 *
 * Author: Michael Windels.
 */
module distribute.entry;

import std.string;
import std.traits;
import std.typecons;
import distribute.utils;

private:

/**
 * Returns a string representing a member field for the Params union converted from a member function of another type.
 * Used in conjunction with forAllMembers at compile time.
 *
 * Returns: A field declaration derived from the symbol.
 */
static string _createParamField(alias symbol)(immutable string) pure @safe
if(isFunction!symbol || isDelegate!symbol)
{
	//Start by defining a new parameter type tuple.
	string decl = "Tuple!(";
	
	//Add the members of the parameter tuple.
	//This is necessary because Parameters!symbol will return storage classes (which we don't want).
	//We also make every member immutable, as an Entry is immutable to begin with.
	alias pTypes = Parameters!symbol;
	static foreach(immutable idx; 0 .. pTypes.length)
	{
		decl ~= "immutable " ~ pTypes[idx].stringof;
		static if(idx + 1 < pTypes.length)
		{
			decl ~= ", ";
		}
	}
	
	//Complete the type and add the field name.
	decl ~= ") " ~ ConsistentMangle!symbol ~ ";";
	return decl;
}


package:

/**
 * Represents a multi-paxos log entry for some data type T which is being distributed.
 */
immutable struct Entry(T)
{
	/**
	 * Represents all possible parameter tuples for the operations of T.
	 */
	immutable union Params
	{
		Tuple!() get;
		Tuple!(immutable T) set;
		
		mixin(forAllMembers!(T, _createParamField, nop, nop)());
	}
	
	string[] node;
	string operation;
	Params arguments;
	
	/**
	 * Initializes an Entry using symbol, a member function of T.
	 */
	this(alias symbol)(immutable string[] n, immutable Parameters!symbol args)
	if(isFunction!symbol || isDelegate!symbol)
	{
		node = n;
		operation = ConsistentMangle!symbol;
		mixin("arguments." ~ ConsistentMangle!symbol) = Tuple!(mixin("arguments." ~ ConsistentMangle!symbol ~ ".Types"))(args);
	}
	
	/**
	 * Initializes an Entry for the get command.
	 */
	this(string name)(immutable string[] n)
	if(name == "get")
	{
		node = n;
		operation = name;
		arguments.get = Tuple!()();
	}
	
	/**
	 * Initializes an Entry for the set command.
	 */
	this(string name)(immutable string[] n, immutable T arg)
	if(name == "set")
	{
		node = n;
		operation = name;
		arguments.set = Tuple!(immutable T)(arg);
	}
}
unittest
{
	class Example
	{
		void a() {}
		float b(int x) {return 1.0;}
		ref string c(ref string x, char y, char z) {return x;}
	}
	
	void testGet() {}
	void testSet(Example) {}
	
	void test(T, alias symbol)(Parameters!symbol args)
	if(isFunction!symbol || isDelegate!symbol)
	{
		immutable string[] node;
		
		static if(symbol.mangleof == testGet.mangleof)
		{
			Entry!T e = Entry!T().__ctor!"get"(node);
			assert(e.operation == "get");
			assert(e.arguments.get.length == 0);
		}
		else static if(symbol.mangleof == testSet.mangleof)
		{
			immutable Example ex = new immutable(Example);	//Will need to come up with a scheme to deep-copy arbitrary data structures.
			Entry!T e = Entry!T().__ctor!"set"(node, ex);
			assert(e.operation == "set");
			assert(tuple(ex) == e.arguments.set);
		}
		else
		{
			immutable Parameters!symbol iArgs = args;
			Entry!T e = Entry!T().__ctor!symbol(node, iArgs);
			assert(e.operation == ConsistentMangle!symbol);
			assert(tuple(args) == mixin("e.arguments." ~ ConsistentMangle!symbol));	//Normally, one would use a switch statement on e.operation rather than a mixin like this.
		}
	}
	
	string s = "hi";
	
	test!(Example, Example.a)();
	test!(Example, Example.b)(1);
	test!(Example, Example.c)(s, cast(char)1, cast(char)-1);
	test!(Example, testGet)();
	test!(Example, testSet)(new Example);
}

void main()
{
	
}