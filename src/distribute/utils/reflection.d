/**
 * Provides some frequently needed reflection capabilities and mixin utilities.
 *
 * Author: Michael Windels.
 */
module distribute.utils.reflection;

import std.traits;
import distribute.distributed_base;
import distribute.utils.tmp;

package(distribute):

/**
 * Function which returns an empty string.
 * Meant to be used in conjunction with forAllMembers if no code is desired for function, field, or type members.
 *
 * Returns: An empty string.
 */
static string nop(alias _)(immutable string) pure @safe nothrow @nogc
{
	return "";
}

/**
 * Generates code (as a string) by iterating over all the members of some type T.
 * The forFunction, forField, and forType template parameters are assumed to be symbols representing functions which:
 * 	- Return a string.
 * 	- Take one alias template parameter (a member symbol).
 * 	- Take one immutable string parameter (a member name).
 * All three functions must be callable at compile time, as this function is intended to be called at compile time.
 *
 * Returns: A string of code generated by applying the forFunction, forField, and forType functions over the members of type T.
 */
static string forAllMembers(T, alias forFunction, alias forField, alias forType)() pure @safe
if(__traits(isTemplate, forFunction) && __traits(isTemplate, forField) && __traits(isTemplate, forType))
{
	string code = "";
	
	static if(is(T == class) || is(T == struct) || is(T == union))
	{
		static foreach(immutable member; __traits(allMembers, T))
		{
			static if(member != "this")	//Ignore the context pointer (if it exists).
			{
				static if(member.length < 2 || !(member[0] == '_' && member[1] == '_'))	//Ignore double-underscore members, they're reserved.
				{
					static if(!__traits(isTemplate, __traits(getMember, T, member)))	//Ignore templates, they return no type (void).
					{
						static if(!__traits(hasMember, Distributed_Base!T, member))
						{
							static if(isFunction!(__traits(getMember, T, member)) || isDelegate!(__traits(getMember, T, member)))
							{
								static foreach(immutable overload; __traits(getOverloads, T, member))
								{
									static if(IsAccessible!overload)
									{
										code ~= forFunction!overload(member);
									}
								}
							}
							else
							{
								static if(IsAccessible!(__traits(getMember, T, member)))
								{
									static if(is(__traits(getMember, T, member)))
									{
										code ~= forType!(__traits(getMember, T, member))(member);
									}
									else
									{
										code ~= forField!(__traits(getMember, T, member))(member);
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	return code;
}