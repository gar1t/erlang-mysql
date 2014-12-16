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

```
dbapi:execute(Db, Query) -> {ok, Result} | {error, Err}
dbapi:result_rows(Result) -> Rows
dbapi:result_next(Result) -> {Next, Result2} | eof
dbapi:result_prev(Result) -> {Prev, Result2} | bof
dbapi:result_attr(Result, Name) -> Value
```

Or more compactly:

```
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

```
dbapi:describe(Result, Name) -> Value
```

By default this would just return `proplist:get_value(describe(Term), Name)` -
but the module could implement optimized forms (to avoid creating the
proplist).

Additionally, the spec could require the module to support various `column_xxx`
properties, which applied a map to the columns in a result set (or undefined
otherwise). E.g. `column_names` would return a map of column names for a
resulset.

## Error Details

Using a record named dberr to provide an error result in this form:

    {error, Error}

The record has these fields:

`sqlstate`
: The
[SQLSTATE error code](http://raymondkolbe.com/2009/03/08/sql-92-sqlstate-codes/). This
field is spelled out, rather than use, e.g. `state` or `code` to make it
completely obvious what this. The term `state` is ambiguous and collides with
Erlang state nomenclature. If you search for "sqlstate" you get what this
is. The extra few characters I think is worth it to clarify what this is.

`native`
: This is the native error. The term `native` is used to avoid using anything
that might overlap with `sqlstate` - so e.g. `code` and `error` are not good
options.

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

Given this is a record, we could add an `extra` field (proplist or map) without
impacting code/usage. It'd be nice however to use this spec without requiring
any code downloads - i.e. this is an _easy to implement_ spec.

Without the record definition, an error handler might look like this:

``` erlang
handle_result({ok, Result}) ->
    Result.
handle_result({error, {dberr, "42000", _, _}}) ->
    error("Invalid syntax");
```

The third element, as `msg` could be abused to include a map or proplist.

I'm inclined to stick with the limitation of a simple three-element record.

## Representing NULL

We should use the atom `null` to represent NULL (as opposed to `nil`,
`undefined`, etc).
