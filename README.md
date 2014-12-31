# Goals

- The go-to library for using MySQL from an Erlang application
- First pass "DB API" interface for Erlang

# Approach

Implement support for the MySQL client protocol:

   [MySQL wire protocol](http://dev.mysql.com/doc/internals/en/client-server-protocol.html)

- Not handling servers that don't support CLIENT_PROTOCOL_41
- Deferring SSL support until we have plain working
- Not supporting CLIENT_CONNECT_ATTRS (what does this buy us?)

Areas of responsibility:

- Network (wrap socket interface) - `mysql_net`
- MySQL client protocol - `mysql_protocol`
- Authentication (use network and protocol) - `mysql_auth`
- MySQP API (use network, protocol, and auth) - `mysql_lib`
- DB API (wrap calls to MySQL API) - `mysql`

# Behavior

These are notes about working decisions and thinking - easily changed.

## Connection pools

If connection pools are supported, they should be layered on top of a
straight-forward single connection interface.

## Network errors

Any network errors should be generated as exceptions. Connection sockets should
be allowed to be cleaned up in a crash as per the Erlang socket behavior.

Reconnects should be handled explicitly by clients. This could be provided by a
layer at the DBAPI level that marshals call to the mysql module and handles
reconnects automatically. This would require a supervised process hierarchy
that's out of scope for the MySQL library itself.

## Data conversion

The MySQL text protocol returns row values as strings. While we know the type
and it's nice to have the strings converted to their corresponding native
Erlang types, I wonder if we want to do this?

- It's expensive
- We have no idea how the value will be used - a string might be fine
- In many cases, numbers will be converted back into strings anyway

This might be a case of over-thinking, but do we want to hard-code this expense
into the API? It's trivial to otherwise expose "string to native value"
conversion functions, which can be used explicitly.

Note that the binary prepared statement protocol provides results as encoded
native values, which is far more efficient.

## Concurrent Access

The library assumes one process acting on a connection at any one time.

## Async vs Sync Interface

The PostgreSQL Erlang bindings provide two interfaces: asynchronous and
synchronous. The MySQL library (and the DB API) defines a synchronous interface
that is assumed to be used by one process at a time.

# DBAPI

## Query Results

The MySQL protocol supports these results for a query:

- OK packet (affected rows, last insert ID, flags, warnings, info)
- Error packet (sql state, code, message)
- Result set packet (columns, rows)

We can easily divide these into the standard {ok, _} and {error, _} results.

How should we represent the "OK" vs the "Result set" packets?

The Erlang ODBC interface doesn't tag results with `ok` and `error` as
two-tuples, but instead provides record like tuples:

- `{updated, N}`
- `{selected, Cols, Rows}`
- `{error, Msg}`

While simple, this API discards a lot of information from the result. I think
we need an interface that captures everything, while remains simple for typical
use.

It looks like we're dealing with two different success cases: one that returns
query results (select) and one that does not (insert, update, use, etc).

Note that the Python DB API uses a "cursor" to provide a consistent interface:

- The `rowcount` attribute acts as the "affected rows" value and doubles as a
  result row count for select operations and as affected rows for non-select
  operations.

- The `description` attributes provides column information for select
  operations and is None for non-select operation.

- The navigation functions (`next`, `prev`, etc.) allow for row iteration and
  retrieval.

We could implement the same pattern in Erlang:

```erlang
dbapi:execute(Db, Query) -> {ok, Result} | {error, Err}
dbapi:result_rows(Result) -> Rows
dbapi:result_next(Result) -> {Next, Result2} | eof
dbapi:result_prev(Result) -> {Prev, Result2} | bof
dbapi:result_attr(Result, Name) -> Value
```

Or more compactly:

```erlang
dbapi:execute(Db, Query) -> {ok, Result} | {error, Error}
dbapi:rows(Result) -> Rows
dbapi:next(Result) -> {Next, Result2} | eof
dbapi:prev(Result) -> {Prev, Result2} | bof
dbapi:describe(Result | Error) -> Props
```

This would let the library hide all record definitions. It could use the native
representation of the results, letting clients use those directly if they
wanted to. Otherwise, it would serve the data via these additional functions.

We could also provide an optimization/convenience form of describe:

```erlang
dbapi:describe(Result, Name) -> Value
```

By default this would just return `proplist:get_value(describe(Term), Name)` -
but the module could implement optimized forms (to avoid creating the
proplist).

Additionally, the spec could require the module to support various `column_xxx`
properties, which applied a map to the columns in a result set (or undefined
otherwise). E.g. `column_names` would return a map of column names for a
resulset.

## API for Pattern Matching

We don't want to hide everything behind functions as this would cut against
Erlang's convention of handling results via pattern matching.

E.g. this goes away:

```erlang
get_users(Db) ->
    handle_users_select(mysql:execute(Db, "select * from users")).

handle_users_select({ok, Users}) when length(Users) == 0 ->
    no_users;
handle_users_select({ok, Users}) ->
    format_users(Users);
handle_users_select({error, Err}) ->
    error({select_users, Err}).
```

Instead, you'd need to call `mysql:rows` first and use its results, but only
for success cases. So users will end up writing functions like this:

```erlang
rows({ok, Result}) -> {ok, mysql:rows(Result)};
rows({error, Error}) -> {error, Error}.
```

## Prepared Statements

A prepared statement is like a text query on the surface. But MySQL implements
a very different protocol for it:

- A query is first "prepared" on the server
- Operations are performed using a reference to the statement, which lives on
  the server
- The statement represents lifecycle must be managed by the client
- The inbound parameters and result set is encoded as native types rather than
  text

At first glance, the prepared statement interface should be reflected
differently. However, consider these points:

- The ODBC supports perpared statements via `param_query` but hides their
  lifecycle - it's not clear if prepared statements are reused (though
  obviously they should be, it's not clear from the docs)

- The Python DB API recommends that libraries reuse prepared statements as an
  optimization, but otherwise hides their lifecycle (i.e. no explicit "release"
  or "free" interface)

Our options:

- Hide the prepared statement functionality behind a "smart" query facility
  (this is what the Python DB API spec calls for)
- Fully expose the prepared statement interface (the PostgeSQL library supports
  this)
- Expose the "prepare" functionality but otherwise ignore the lifecycle issues

### Option 1: Hide Statements - Use Implicitly

The Python DB API is the model for this approach. It provides a single
`execute` function that accepts an optional list of parameters. If the
parameters are provided, the library may use that as an opportunity to first
prepare a statement using the query arg and then execute that statement using
the parameters. The library may reuse a previously prepared statement (using a
cache) for subsequent calls using the same query (compare using a hash).

This is a nice approach in that it hides complexity and maintains a single
interface for both so called "simple queries" and prepared statements.

It however places this responsibility on the library:

- Implicitly prepare a statement when parameters are provided (and not already
  cached)

- Cache queries (this requires a managed caching facility or lazy use of
  ets - both of these are complicated)

- Address "maximum cached prepared statements" to avoid the theoretical
  possibility of running out of resources (while this is not likely and does
  not aopear to be addressed in any of the libraries that hide this
  functionality, it's still a point of consideration)

### Option 2: Exerything Explicit

In this case the client would assume responsbility for preparing statements and
using the statement references in place of the text based queries.

It would require functions to explicitly create and delete statements:

```erlang
dbapi:prepare(Query) -> {ok, Statement} | {error, Reason}
dbapi:close_statement(Statement) -> ok
```

We might consider an explicit `close_statement` to differentiate closing a
database connection from freeing memory used by a prepared statement. Or,
alternatively, `free`. We're really talking about a counterpart to `prepare`
(similar to `connect` vs `close`).

## Error Details

It's very common to match against specific error codes. E.g.

```erlang
handle_result({error, {"42000", ...}}) -> handle_permission_error();
handle_result({error, Err}) -> handle_general_error(Err).
```

To support this usage, it's important to:

- Provide one or more values to match
- Avoid long tuples that are typically handled by usine a record def
- Avoid extra noise (in particular record tags)

Cases to avoid:

```erlang
handle_result1({error, {"42000", _, _, _}}) -> handle_permission_error().

handle_result2({error, {dberr, "42000", _, _}) -> handle_permission_error().

handle_result3({error, #dberr{code="42000"}) -> handle_permission_error().
```

The last case I think is important to avoid as it starts to suggest that the
API must ship with supporting code. I'd like to avoid this. The interface
should be easy to implement without any shared libraries. This is similar to
Python's approach and it will work fine with Erlang, given some thought.

We obviously want to provide error values in the form `{error, Reason}`.

The payload should contain, at a minimum, the fields: `sqlstate`,
`native_code`, and `msg`.

`sqlstate`
: The
[SQLSTATE error code](http://raymondkolbe.com/2009/03/08/sql-92-sqlstate-codes/). This
field is spelled out, rather than use, e.g. `state` or `code` to make it
completely obvious what this. The term `state` is ambiguous and collides with
Erlang state nomenclature. If you search for "sqlstate" you get what this
is. The extra few characters I think is worth it to clarify what this is.

`native_code`
: This is the native error code. The term `native_code` is used to make it
completely obvious that we're not dealing with `sqlstate` and that the value is
a code that's tied to the implementation (native).

`msg`
: The textual representation of the error.

These three fields will accommodate the error specifications for these
libraries/APIs:

- ODBC ([see spec](http://msdn.microsoft.com/en-us/library/ms716256%28v=vs.85%29.aspx))
- JDBC ([see spec](http://docs.oracle.com/javase/7/docs/api/java/sql/SQLException.html))
- Emysql
- PostreSQL ([see spec](http://www.postgresql.org/docs/9.3/static/plpgsql-errors-and-messages.html))

PosgreSQL provides some other information, which this spec would not
accommodate:

- DETAIL
- HINT
- COLUMN
- CONSTRAINT
- DATATYPE
- TABLE
- SCHEMA

These are all arguably fields that could be encoded in a single `msg` field
however.

If we structured the error as follows, we could accommodate as much detail as
needed:

    `{error, {SQLState, Detail}=Error}`

To get the property list, which contains the native code, message, and anything
else provided by the driver, we could use `describe`:

```erlang
mysql:describe(Error, native_code)
```

Providing a two-tuple with `sqlstate` as the first element enables support for
simply code-based pattern matching (see API for Pattern Matching above).

## Representing NULL

We should use the atom `null` to represent NULL (as opposed to `nil`,
`undefined`, etc).

## Time

We need to officially support microsecond components of timestamp
values. What's the right way to do this in Erlang? The erlang:now/0 type
supports this, but there's a cost to convert from the database format, which is
is more likely to resemble the calendar datetime format.

Making this a TODO for now.

## Handling Connection Errors

Because we're maintaining an open socket connection as a part of the client
reference, the client reference will become invalid/corrupt when that
connection is lost. Similarly, any network error can have this effect.

At the moment, any connection errors must be handled by the client code. In the
trivial case, the client will crash and be restarted, causing the operation to
fail. Subsequent operations will succeed if the restarted process can
reestablish a connection. This is obviously not acceptable and we need a
reasonable strategy for handling connection errors or other problems with the
server.

Options:

1. Do nothing and let the client implement the desired behavior
2. Use a process or processes within the DB API implementation to implement
   reconnects
3. Build another layer with the erlang-mysql library (but not a part of
   the DB API spec) to handle reconnects

Note that Emysql handles reconnects very nicely. However, it's an active
application, requiring a supervisory hierarchy to run. This is too much to
require IMO. I think this elmininates option 2.

Option 1 is alwasy at hand as long as we don't complicate the library with
built-in reconnect behavior.

Option 3 seems the most viable. While it's tempting to consider this in
conjunction with a connection pool facility, let's leave that alone for now.

We'd want to preserve the interface so that a supervised auto-reconnecting
connection is used the same as one that is not. I.e. mysql:xxxx functions
should work exactly the same.

Playing around with a possibility:

```erlang
%% Create a supervising process.

{ok, Sup} = mysql_sup:start_link(),

%% Open a connection via the supervisor - Options include the normal
%% connect options plus supervisor options such as retry counts, policies,
%% etc.

{ok, Db} = mysql_sup:connect(Sup, Options),

%% Use the connection in the normal way. At this point, lost connections
%% are handled seamlessly (with some additional initial delay to restablish
%% the lost connection.

mysql:execute(Db, Query)
```

This is a lovely idea, but how on earth can we sanely pull it off? The current
`mysql` module naively passes operations through to `mysql_lib`. The `Db` value
in this case is a record `#mysql{sock}` where the socket is used in calls to
`mysql_lib`.

We could redefine the record as `#mysql{sock, libmod}` and use `mysql_lib` as
the default. `mysql_lib` would then implement perhaps a `mysql_lib` behavior.

The supervised version would provide an alternative libmod -- e.g.
`mysql_sup_lib`, which would wrap calls to `mysql_lib`. The sock would be a
reference to a socket managed by the supervising process.

Note that this `_sup` here is not a real Erlang supervisor, but behaves like
one. The implementation would be a gen_server, more likely. This could be a
little confusing and might be the wrong way to spell this.

This might want to be a "pool" concept. So, alternatively:

```erlang
%% Start a "pool" process, which has both connect options for a particular
%% host/db and additional options for the pool (e.g. max connection count,
%% reconnect options, etc.) Real connections would be lazily created by calls
%% to `mysql_pool:get_connection`.

{ok, Pool} = mysql_pool:start_link(Options),

%% Get a connection to the database. If one was not available, the pool would
%% open a new connection (up to max) and provide it as `#mysql{sock, libmod}`
%% where `sock` is a reference to a connection socket that can be updated
%% seamlessly in the background on reconnects and `libmod` is a wrapper to
%% `mysql_lib` that implements reconnect + retry logic based on the pool
%% options (e.g. max retries, retry delays, etc.)

{ok, Db} = mysql_pool:get_connection(Pool),

%% The client uses the Db reference as any other, not needing to know the
%% trickier behavior going on behind the scenes.

mysql:execute(Db, Query)
```

# To Do

## Evaluate mysql_lib for protocol logic and move to mysql_protocol

`mysql_lib` is meant to be a higher level interface to MySQL that coordinates
network (`mysql_net`) and protocol encoding/decoding (`mysql_protocol`). The
cross packet decoding that it does currently should probably be in
`mysql_protocol`. The protocol itself could be made easier to follow if it
surfaced as a series of receiever/decode functions that were applied along with
a `next_packet` or `recv` callback (to decouple the network function).

## Move network access out of `mysql_protocol`

The direct use of `mysql_net` in `mysql_protocol` is not really that great. The
idea of the protocol module is to be a pure encoder/decoder with zero coupling
to the network interface. A good measure of decoupling here is to make
everything in that module testable.

Direct tests would be a good illustration of the protocol use as well.

Another example where the protocol leaks out of `mysql_protocol` is
`mysql_lib:recv_execute_resp_rows` which is forced to work with raw packet data
(MySQL likes to reuse the OK header 16#00 in other cases, making it impossible
to define a context-free set of packet decoders).

## Cursor based row retrieval

We can use cursors with MySQL via the prepared statement protocol:

Setting the cursor:

http://dev.mysql.com/doc/internals/en/com-stmt-execute.html

Retrieving results:

http://dev.mysql.com/doc/internals/en/com-stmt-fetch.html

## Finalize timestamp format

Options:

1. Use erlang:now/0 type: {MegaSeconds, Seconds, MicroSeconds}
2. Use calendar datetime: {{Year, Month, Day}, {Hour, Min, Sec}}

Option 1 doesn't support dates before 1970, so I think we have to strike this.

Option 2 doesn't support microseconds.

Adding a fourth element to the time tuple is a bit strange and would make
working with this type a real pain.

The seconds element could optionally be a float, but this feels like it could
lose precision (maybe false?)

Note that the MySQL implementation accepts both formats in (options 1 and 2)
but only provides option 2 out (not including the microsecond component - this
isn't coming back from MySQL for some reason - either a bug on our side or not
using the right type or precision).

## Multiple Statements

Python DB API supports executemany - an explicit function to execute multiple
statements. Emysql seems to implicitly support it - you separate statements
with a semi-colon and it just works.

I'm surprised this doesn't "just work" with our current text query
implementation.

## Timeouts

How do we handle timeouts. Connect timeout is easy enough and it currently
accommodated (not tested). What about execute timeouts?

