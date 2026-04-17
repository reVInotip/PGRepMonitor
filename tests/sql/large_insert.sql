do $$
begin
    for i in 1..1000000 loop
        insert into test_table values (1);
    end loop;
end $$;