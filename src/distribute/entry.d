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
 * Returns a string representing a member field for the Params union converted from a mutable member function of another type.
 * Used in conjunction with forAllMembers at compile time.
 *
 * Returns: A field declaration derived from the symbol.
 */
static string _createParamField(alias symbol)(immutable string) pure @safe
if(isFunction!symbol || isDelegate!symbol)
{
	//Only return a new field declaration if the symbol is mutable (or non-const inout).
	static if(!is(ConstOf!(typeof(symbol)) == typeof(symbol)) && !is(ImmutableOf!(typeof(symbol)) == typeof(symbol)))
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
	else
	{
		return "";
	}
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
		mixin(forAllMembers!(T, _createParamField, nop, nop)());
		
		static if(__traits(allMembers, Params).length == 0)
		{
			Tuple!(immutable T) set;
		}
	}
	
	string[] node;
	string operation;
	Params arguments;
	
	/**
	 * Initializes an Entry using symbol, a mutable member function of T.
	 */
	this(alias symbol)(immutable string[] n, immutable Parameters!symbol args)
	if(isFunction!symbol || isDelegate!symbol)
	{
		node = n;
		operation = ConsistentMangle!symbol;
		mixin("arguments." ~ ConsistentMangle!symbol) = Tuple!(mixin("arguments." ~ ConsistentMangle!symbol ~ ".Types"))(args);
	}
	
	static if(__traits(hasMember, Params, "set"))
	{
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
}
unittest
{
	class Example
	{
		void a() {}
		float b(int x) inout {return 1.0;}
		ref string c(ref string x, char y, char z) {return x;}
		int d(bool x, string y) const {return 1;}
		@(1, "yes") void e() immutable {}
	}
	
	void testSet(T)(T) {}
	
	void test(T, alias symbol)(Parameters!symbol args)
	if(isFunction!symbol || isDelegate!symbol)
	{
		immutable string[] node;
		
		static if(symbol.mangleof == (testSet!T).mangleof)
		{
			immutable T iArg = cast(immutable)args;	//May need to come up with a scheme to deep-copy arbitrary data structures.
			Entry!T e = Entry!T().__ctor!"set"(node, iArg);
			assert(e.operation == "set");
			assert(tuple(iArg) == e.arguments.set);
			assert(__traits(allMembers, Entry!T.Params).length == 1);
		}
		else
		{
			immutable Parameters!symbol iArgs = args;
			Entry!T e = Entry!T().__ctor!symbol(node, iArgs);
			assert(e.operation == ConsistentMangle!symbol);
			assert(tuple(args) == mixin("e.arguments." ~ ConsistentMangle!symbol));	//At run time, one should use a switch statement on e.operation rather than a mixin like this.
			assert(!__traits(compiles, Entry!T().__ctor!"set"));
		}
	}
	
	string s = "hi";
	
	test!(Example, Example.a)();
	test!(Example, Example.b)(1);
	test!(Example, Example.c)(s, cast(char)1, cast(char)-1);
	assert(!__traits(compiles, test!(Example, Example.d)));
	assert(!__traits(compiles, test!(Example, Example.e)));
	test!(int, testSet!int)(8);
}