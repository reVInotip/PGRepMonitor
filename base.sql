begin;
--SET LOCAL statement_timeout = 200;
--SET LOCAL lock_timeout = 200;
--SET LOCAL idle_in_transaction_session_timeout = 200;
--SET LOCAL wal_sender_timeout = 200;
create table abobus (id integer, acsdsdc text);
commit;

begin;
--SET LOCAL statement_timeout = 200;
--SET LOCAL lock_timeout = 200;
insert into abobus values (1, 'assdlv,');
commit;

begin;
insert into abobus values (2, 'assdlv,');
commit;

begin;
drop table abobus;
commit;