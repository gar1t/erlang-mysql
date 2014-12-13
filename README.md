# Goals

- The go-to library for using MySQL from an Erlang application
- First pass "DB API" interface for Erlang

# Approach

## Phase 1 - Wrap [Emysql](https://github.com/Eonblast/Emysql)

Just get the API defined and working and sane. Use the venerable product of
time and chance, Emysql.

## Phase 2 - Implement [MySQL wire protocol](http://dev.mysql.com/doc/internals/en/client-server-protocol.html)

# DBAPI

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
