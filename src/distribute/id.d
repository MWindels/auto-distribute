/**
 *
 * Author: Michael Windels.
 */
module distribute.id;

package:

/**
 * Represents a proposal ID.
 */
struct ID
{
	uint round;
	uint node;
	
	/**
	 * Initializes a new ID.
	 *
	 * Params:
	 * 	r = The round of the new ID.
	 * 	n = The node of the new ID.
	 */
	this(uint r, uint n)
	{
		round = r;
		node = n;
	}
	
	/**
	 * Compares two ID objects.
	 *
	 * Params:
	 * 	other = The other ID being compared.
	 *
	 * Returns: Negative if this is lower than other, zero if they're identical, positive if this is higher than other.
	 */
	int opCmp(ID other) const
	{
		if(round < other.round)
		{
			return -1;
		}
		else if(round > other.round)
		{
			return 1;
		}
		else
		{
			if(node < other.node)
			{
				return -1;
			}
			else if(node > other.node)
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}
	}
	
	/**
	 * Performs a binary operation on an ID's round number and an unsigned integer.
	 * Currently supports addition.
	 *
	 * Params:
	 * 	rnd = The unsigned integer operand.
	 *
	 * Returns: An ID whose round number is round op rnd.
	 */
	ID opBinary(string op)(uint rnd) const
	if(op == "+")
	{
		return ID(mixin("round " ~ op ~ " rnd"), node);
	}
	
	/**
	 * Performs a binary operation on an ID's round number and another ID's round number.
	 * See unsigned integer overload for details.
	 */
	ID opBinary(string op)(ID other) const
	{
		return opBinary!op(other.round);
	}
	
	/**
	 * Performs an assignment operation on an ID's round number and an unsigned integer.
	 * Currently supports addition assignment.
	 *
	 * Params:
	 * 	rnd = The unsigned integer operand.
	 */
	void opOpAssign(string op)(uint rnd)
	if(op == "+")
	{
		mixin("round " ~ op ~ "= rnd;");
	}
	
	/**
	 * Performs an assignment operation on an ID's round number and another ID's round number.
	 * See unsigned integer overload for details.
	 */
	void opOpAssign(string op)(ID other)
	{
		opOpAssign!op(other.round);
	}
}
unittest
{
	assert(ID(1, 1) < ID(2, 1));
	assert(ID(1, 1) < ID(1, 2));
	assert(ID(1, 1) <= ID(2, 1));
	assert(ID(1, 1) <= ID(1, 2));
	assert(ID(1, 1) <= ID(1, 1));
	assert(ID(1, 1) == ID(1, 1));
	assert(ID(1, 1) != ID(1, 2));
	assert(ID(1, 1) != ID(2, 1));
	assert(ID(1, 1) >= ID(1, 1));
	assert(ID(1, 2) >= ID(1, 1));
	assert(ID(2, 1) >= ID(1, 1));
	assert(ID(1, 2) > ID(1, 1));
	assert(ID(2, 1) > ID(1, 1));
	
	assert(ID(1, 1) + 1 == ID(2, 1));
	assert(ID(2, 2) + 10 == ID(12, 2));
	
	assert(ID(1, 1) + ID(1, 1) == ID(2, 1));
	assert(ID(2, 2) + ID(10, 1) == ID(12, 2));
	
	ID i = ID(1, 1);
	assert(i == ID(1, 1));
	i += 1;
	assert(i == ID(2, 1));
	i += 10;
	assert(i == ID(12, 1));
	
	ID j = ID(1, 1);
	assert(j == ID(1, 1));
	j += ID(1, 2);
	assert(j == ID(2, 1));
	j += ID(10, 1);
	assert(j == ID(12, 1));
	
	immutable ID k = ID(1, 1);
	assert(k < ID(2, 1));
	assert(k + 1 == ID(2, 1));
	assert(k + ID(1, 1) == ID(2, 1));
}