/**
 * Provides some template meta-programming utilities.
 *
 * Author: Michael Windels.
 */
module distribute.utils.tmp;

import std.string;

package(distribute):

/**
 * Determines whether symbol is accessible outside of the distribute package.
 */
enum bool IsAccessible(alias symbol) = __traits(getProtection, symbol) == "public" || __traits(getProtection, symbol) == "export";

/**
 * Provides a mangled name which is consistent across D implementations.
 * Strips leading underscores from a symbol's mangled name as they're implementation-defined (see https://dlang.org/spec/property.html#mangleof for details).
 */
enum string ConsistentMangle(alias symbol) = strip(symbol.mangleof, "_", "");