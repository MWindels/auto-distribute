/**
 * Provides some threading-related utilities.
 *
 * Author: Michael Windels.
 */
module distribute.utils.thread;

import std.concurrency;
import std.variant : Variant;

package(distribute):

/**
 * Waits until all Tids provided have been received exactly once.
 * Assumes all provided Tids were sent concurrently.
 *
 * Params:
 * 	tids = A series of Tids to wait for.
 */
static void waitForTids(const Tid[] tids...)
{
	shared(Tid)[] tidResends = [];
	shared(Variant)[] varResends = [];
	
	bool[const Tid] received;
	foreach(tid; tids)
	{
		received[tid] = false;
	}
	
	for(int i = 0; i < tids.length; ++i)
	{
		bool recv = false;
		while(!recv)
		{
			receive(
				(Tid t)
				{
					bool* tRecv = t in received;
					if(tRecv != null)
					{
						if(*tRecv)
						{
							tidResends ~= [cast(shared)t];
						}
						else
						{
							*tRecv = true;
							recv = true;
						}
					}
					else
					{
						tidResends ~= [cast(shared)t];
					}
				},
				(Variant v)
				{
					varResends ~= [cast(shared)v];
				}
			);
		}
	}
	
	foreach(t; tidResends)
	{
		spawn(() shared {send(ownerTid, cast(Tid)t);});
	}
	foreach(v; varResends)
	{
		spawn(() shared {send(ownerTid, cast(Variant)v);});
	}
}