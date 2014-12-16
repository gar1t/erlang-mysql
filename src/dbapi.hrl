-type dbapi_connect_options() :: [dbapi_connect_option()].

-type dbapi_connect_option() ::
        {host, string()} |
        {user, string()} |
        {password, string()} |
        {database, string()} |
        {dsn, string()}.

-type dbapi_db() :: term().

-type dbapi_connect_error_reason() :: term().
