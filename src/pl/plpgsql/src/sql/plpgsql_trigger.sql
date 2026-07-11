-- Simple test to verify accessibility of the OLD and NEW trigger variables

create table testtr (a int, b text);

create function testtr_trigger() returns trigger language plpgsql as
$$begin
  raise notice 'tg_op = %', tg_op;
  raise notice 'old(%) = %', old.a, row(old.*);
  raise notice 'new(%) = %', new.a, row(new.*);
  if (tg_op = 'DELETE') then
    return old;
  else
    return new;
  end if;
end$$;

create trigger testtr_trigger before insert or delete or update on testtr
  for each row execute function testtr_trigger();

insert into testtr values (1, 'one'), (2, 'two');

update testtr set a = a + 1;

delete from testtr;

-- Verify that NEW record fields used as SQL parameters get valid types.
-- This exercises the per-session record typcache tupledesc counter and the
-- per-execution PL/pgSQL recfield cache.
create table testtr_param (a int primary key, b int);
create table testtr_param_log (a int, b int);

create function testtr_param_trigger() returns trigger language plpgsql as
$$begin
  insert into testtr_param_log values (new.a, new.b);
  return new;
end$$;

create trigger testtr_param_trigger after insert or update on testtr_param
  for each row execute function testtr_param_trigger();

begin;
insert into testtr_param values (1, 10);
update testtr_param set b = b + 1 where a = 1;
select count(*) as before_commit from testtr_param_log;
commit;
select count(*) as after_commit, sum(b) as sum_b from testtr_param_log;
