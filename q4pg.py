from contextlib import contextmanager
import select, json, re, psycopg2
import psycopg2.extensions

LISTEN_TIMEOUT_SECONDS = 60 # 1min
NEW, PROCESSING, FINISHED, FAILED = 1, 2, 3, 4

class QueueManager(object):


    def __init__(self,
                 dsn="", table_name="mq",
                 data_type="json",
                 data_length=1023,
                 excepted_times_to_ignore=0):
        self.parse_dsn(dsn)
        self.table_name   = table_name
        self.data_type    = data_type
        self.data_length  = data_length
        self.serializer   = lambda d: d
        self.deserializer = lambda d: d
        self.excepted_times_to_ignore = excepted_times_to_ignore
        if data_type is "json":
            self.serializer   = lambda d: json.dumps(d, separators=(',',':'))
            self.deserializer = lambda d: json.loads(d)
        self.setup_sqls()
        self.invoking_queue_id = None

    def parse_dsn(self, dsn):
        if dsn == "": # to use other session.
            self.dsn = None
        elif type(dsn) == str:
            mat = re.match(r'^(.+)://(.+?)(?::(.*)|)@(.+?)(?::(.*?)|)/(.+)', dsn)
            if mat: # is it url arg? (driver://username:password@hostname:port/dbname)
                driver, username, password, hostname, port, dbname = map(lambda i: mat.group(i), xrange(1,7))
                if not (driver in ('postgresql', 'postgres', 'psql', )):
                    raise Exception("Invalid driver (%s). QueueManager supports only 'postgresql://'." % driver)
                self.dsn = "user=%s host=%s dbname=%s" % (username, hostname, dbname, )
                self.dsn += (" port=%s" % port if port else "")
                self.dsn += (" password=%s" % password if password else "")
            else:
                self.dsn = dsn # psycopg2 arg.
        else:
            raise Exception("Invalid dsn argument given (%s)." % str(dsn))

    @contextmanager
    def session(self, other_sess):
        conn = None
        cur  = None
        if other_sess:
            cur  = other_sess
            yield (conn, cur)
        else:
            try:
                conn = psycopg2.connect(self.dsn)
                cur  = conn.cursor()
                yield (conn, cur)
            except:
                if conn and cur and (self.invoking_queue_id != None):
                    cur.execute(self.report_sql % (self.invoking_queue_id,))
                    res = cur.fetchone()
                    if res and res[0]:
                        conn.commit()
                raise
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
        return

    def setup_sqls(self):
        n = self.table_name
        self.create_table_sql = """
create table %s (
    id             serial          primary key,
    tag            varchar(31)     not null,
    content        varchar(%d),
    created_at     timestamp       not null default current_timestamp,
    except_times   integer         default 0,
    state          integer         default 0
);
create index %s_tag_idx         on %s(tag);
create index %s_created_at_idx  on %s(created_at);
create index %s_state_idx  on %s(state);
""" % (n, self.data_length, n, n, n, n, n, n)
        self.drop_table_sql = """
drop table %s;
""" % (n,)
        self.insert_sql = """
insert into %s (tag, content) values ('%%s', '%%s') returning id;
""" % (n,)
        self.report_sql = """
update %s set except_times = except_times + 1
  where id = %%s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.select_sql = """
update %s set state = %s
  where case when tag = '%%s' then pg_try_advisory_lock(tableoid::int, id) else false end
  and id = %%s
  returning *;
""" % (n, NEW)
        self.list_sql = """
select * from %s
  where case when tag = '%%s' then pg_try_advisory_lock(tableoid::int, id) else false end;
""" % (n,)
        self.count_sql = """
select count(*) from %s
  where case when tag = '%%s' then pg_try_advisory_lock(tableoid::int, id) else false end;
""" % (n,)
        self.cancel_sql = """
delete from %s where id = %%s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.ack_sql = """
delete from %s where id = %%s
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.notify_sql = """
notify %s, '%%s';
"""
        self.listen_sql = """
listen %s;
"""

    def create_table(self, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.create_table_sql)
            if conn: conn.commit()

    def drop_table(self, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.drop_table_sql)
            if conn: conn.commit()

    def reset_table(self, other_sess = None):
        self.drop_table(other_sess)
        self.create_table(other_sess)

    def enqueue(self, tag, data, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.insert_sql % (tag, self.serializer(data),))
            task_id = cur.fetchone()[0]
            cur.execute(self.notify_sql % tag, (task_id,))
            conn.commit()
            return task_id

    def listen_item(self, tag, timeout=None):
        while True:
            with self.session(None) as (conn, cur):
                with self.session(None) as (lconn, lcur):
                    lconn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    lconn.poll()
                    lcur.execute((self.listen_sql % (tag,)))
                    if select.select([lconn],[],[],(timeout or LISTEN_TIMEOUT_SECONDS)) == ([],[],[]):
                        if timeout:
                            self.invoking_queue_id = None # to ignore error reporting.
                            yield None
                        continue
                    lconn.poll()
                    while lconn.notifies:
                        n = lconn.notifies.pop()
                        cur.execute(self.select_sql  % (tag, int(n.payload),))
                        res = cur.fetchone()
                        yield res

                    '''
                            
                        notifies.append(conn.notifies.pop())
                    notifies.reverse()
                    print notifies
                    for n in notifies:
                        print n.payload
                        cur.execute(self.select_sql  % (tag, int(n.payload),))
                        res = cur.fetchone()
                        print res
                    notifies = []
                    '''

    def listen_item_orig(self, tag, timeout=None):
        while True:
            with self.session(None) as (conn, cur):
                cur.execute(self.select_sql % (tag,))
                res = cur.fetchone()
                if res:
                    self.invoking_queue_id = res[0]
                    if not ((0 < self.excepted_times_to_ignore) and
                            (self.excepted_times_to_ignore <= int(res[4]))):
                        yield res
                    cur.execute(self.ack_sql % (res[0],))
                    conn.commit()
                    self.invoking_queue_id = None
                    continue
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur.execute((self.listen_sql % (tag,)))
                if select.select([conn],[],[],(timeout or LISTEN_TIMEOUT_SECONDS)) == ([],[],[]):
                    if timeout:
                        self.invoking_queue_id = None # to ignore error reporting.
                        yield None
                    continue
                conn.poll()
                if conn.notifies:
                    notify = conn.notifies.pop()
                    cur.execute(self.select_sql % (tag,))
                    res = cur.fetchone()
                    if res:
                        self.invoking_queue_id = res[0]
                        if not ((0 < self.excepted_times_to_ignore) and
                                (self.excepted_times_to_ignore <= int(res[4]))):
                            yield res
                        cur.execute(self.ack_sql % (res[0],))
                        conn.commit()
                        self.invoking_queue_id = None

    def listen(self, tag, timeout=None):
        for d in self.listen_item(tag, timeout=timeout):
            yield (self.deserializer(d[2]) if d != None else None)

    def cancel(self, id, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.cancel_sql % (id,))
            res = cur.fetchone()
            if res and res[0]:
                if conn: conn.commit()
                return res[0]
            return res[0] if res else False

    def list(self, tag, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.list_sql % (tag,))
            res = cur.fetchall()
            return res

    def count(self, tag, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.count_sql % (tag,))
            res = cur.fetchone()[0]
            return int(res)
