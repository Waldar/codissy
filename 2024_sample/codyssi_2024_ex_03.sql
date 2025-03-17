with cte_data as
(
select split_part(value, ' ', 1) as num
     , split_part(value, ' ', 2)::int as base
--from read_files('/Volumes/waldar/fabien/codyssi/2024_sample/codyssi_2024_03.txt', format => 'text')
  from values ('100011101111110010101101110011 2')
            , ('83546306 10')
            , ('1106744474 8')
            , ('170209FD 16')
            , ('2557172641 8')
            , ('2B290C15 16')
            , ('279222446 10')
            , ('6541027340 8') as t (value)
)
select sum(base)                        as part1
     , sum(conv(num, base, 10)::bigint) as part2
     , aggregate( sequence(1, ceil(log(65, part2)))
                , named_struct('b10', part2, 'b64', '')
                , (acc, x) -> named_struct( 'b10', acc.b10 div 65
                                          , 'b64', case
                                                     when acc.b10 % 65 <= 35
                                                     then conv((acc.b10 % 65)::string, 10, 36)
                                                     when acc.b10 % 65 <= 61
                                                     then chr(acc.b10 % 65 + ascii('a') - 36)
                                                     else translate(acc.b10 % 65 % 10, '234', '!@#')
                                                   end || acc.b64 )
                , acc -> acc.b64 )      as part3
  from cte_data;
