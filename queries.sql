-- eliminate all tables in the public schema
-- This script will drop all tables in the public schema of the PostgreSQL database
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;

--query the tables in the public schema
SELECT id,
       "timestamp",
       user_id,
       price
FROM public.transactions
LIMIT 1000;

SELECT load_batch,
       cum_count,
       cum_avg,
       cum_min,
       cum_max,
       last_updated
FROM public.agg_stats
LIMIT 1000;