-type dbapi_connect_options() :: [dbapi_connect_option()].

-type dbapi_connect_option() ::
        {host, string()} |
        {user, string()} |
        {password, string()} |
        {database, string()} |
        {dsn, string()}.
        
-type dbapi_connect_error_reason() ::
        timeout.

-record(dberr, {sqlstate, native, msg}).
