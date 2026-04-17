do $$
begin
    for i in 1..10 loop
        insert into test_table values (1);
    end loop;
end $$;