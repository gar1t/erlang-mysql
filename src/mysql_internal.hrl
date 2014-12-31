-record(mysql, {lib, sock}).

-record(ok_packet,
        {affected_rows,
         last_insert_id,
         status_flags,
         warnings,
         info}).

-record(err_packet,
        {code,
         sqlstate,
         msg}).

-record(eof_packet,
        {warnings,
         status_flags}).

-record(resultset,
        {column_count,
         columns=[],
         rows=[]}).

-record(coldef,
        {catalog,
         schema,
         table,
         org_table,
         name,
         org_name,
         character_set,
         column_length,
         type,
         flags,
         decimals,
         default_values}).

-record(prepared_stmt,
        {stmt_id,
         column_count,
         param_count,
         warning_count,
         params=[],
         columns=[]}).

-record(raw_packet, {data}).
