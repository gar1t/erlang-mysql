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
