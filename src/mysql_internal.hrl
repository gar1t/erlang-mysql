-record(mysql, {sock}).

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

-record(resultset_packet,
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

-record(raw_packet, {data}).
