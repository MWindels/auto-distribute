/**
 * Contains definitions for multi-paxos log entries, and the results generated after applying them.
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
 * Returns a string representing a member field for the Params union derived from a mutable member function of another type.
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
		alias pTypes = Parameters!symbol;
		static foreach(immutable idx; 0 .. pTypes.length)
		{
			decl ~= fullyQualifiedName!(pTypes[idx]);
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

/**
 * Returns a string representing a member field for the Returns union derived from a mutable member function of another type.
 * Used in conjunction with forAllMembers at compile time.
 *
 * Returns: A field declaration derived from the symbol.
 */
static string _createReturnField(alias symbol)(immutable string) pure @safe
if(isFunction!symbol || isDelegate!symbol)
{
	//Only return a new declaration if the symbol is mutable.
	static if(!is(ConstOf!(typeof(symbol)) == typeof(symbol)) && !is(ImmutableOf!(typeof(symbol)) == typeof(symbol)))
	{
		static if(!is(ReturnType!symbol == void))
		{
			return fullyQualifiedName!(ReturnType!symbol) ~ " " ~ ConsistentMangle!symbol ~ ";";
		}
		else
		{
			return "";
		}
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
struct Entry(T)
{
	/**
	 * Represents all possible parameter tuples for the operations of T.
	 */
	union Params
	{
		mixin(forAllMembers!(T, _createParamField, nop, nop)());
		
		static if(__traits(allMembers, Params).length == 0)
		{
			Tuple!T set;
		}
	}
	
	string[] node;
	string operation;
	Params arguments;
	
	/**
	 * Initializes an Entry using symbol, a mutable member function of T.
	 */
	this(alias symbol)(inout string[] n, inout Parameters!symbol args) inout
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
		this(string name)(inout string[] n, inout T arg) inout
		if(name == "set")
		{
			node = n;
			operation = name;
			arguments.set = Tuple!T(arg);
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
		string[] node;
		
		static if(symbol.mangleof == (testSet!T).mangleof)
		{
			Entry!T e = Entry!T().__ctor!"set"(node, args);
			assert(e.operation == "set");
			assert(tuple(args) == e.arguments.set);
			assert(__traits(allMembers, Entry!T.Params).length == 1);
		}
		else
		{
			Entry!T e = Entry!T().__ctor!symbol(node, args);
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

/**
 * Represents the results generated after applying an Entry.
 */
struct Results(T)
{
	/**
	 * Represents all possible return values for the mutable operations of T.
	 */
	union Returns
	{
		mixin(forAllMembers!(T, _createReturnField, nop, nop)());
	}
	
	//TODO: Add support for return parameters.
	
	Returns ret;
	
	/**
	 * Initializes a new Results with a non-void return.
	 *
	 * Params:
	 * 	r = The return value to store.
	 */
	this(alias symbol)(inout ReturnType!symbol r) inout
	if((isFunction!symbol || isDelegate!symbol) && !is(ReturnType!symbol == void))
	{
		mixin("ret." ~ ConsistentMangle!symbol) = r;
	}
	
	/**
	 * Initializes a new Results with a void return.
	 */
	this(alias symbol)() inout
	if((isFunction!symbol || isDelegate!symbol) && is(ReturnType!symbol == void))
	{}
	
	static if(__traits(hasMember, Entry!T.Params, "set"))
	{
		/**
		 * Initializes a new Results for the set operation.
		 */
		this(string name)()
		if(name == "set")
		{}
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
	
	void test(T, alias symbol, string symName)(Parameters!symbol args)
	{
		static if(symbol.mangleof == (testSet!T).mangleof)
		{
			Results!T res = Results!T().__ctor!"set"();
			assert(!__traits(compiles, res.ret.set));
		}
		else
		{
			T t = new T();
			static if(!is(ReturnType!symbol == void))
			{
				Results!T res = Results!T().__ctor!symbol(mixin("t." ~ symName ~ "(args)"));
				assert(mixin("res.ret." ~ ConsistentMangle!symbol) == mixin("t." ~ symName ~ "(args)"));
			}
			else
			{
				Results!T res = Results!T().__ctor!symbol();
				assert(!__traits(compiles, mixin("res.ret." ~ ConsistentMangle!symbol)));
			}
		}
	}
	
	string s = "hi";
	
	test!(Example, Example.a, "a")();
	test!(Example, Example.b, "b")(1);
	test!(Example, Example.c, "c")(s, cast(char)1, cast(char)2);
	assert(!__traits(compiles, test!(Example, Example.d, "d")));
	//assert(!__traits(compiles, test!(Example, Example.e, "e")));	//Returns void, so its accepted by the void constructor.
	test!(int, testSet!int, "")(10);
}