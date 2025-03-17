with cte_data (magnitudes, signs) as
(
select slice(split(value, '\n'), 1, array_size(split(value, '\n')) - 1)
     , split(element_at(split(value, '\n'), -1), '')
--from read_files('/Volumes/waldar/fabien/codyssi/2025_Atlantis/codyssi_2025_01.txt', format => 'text', WholeText => true)
  from values ('8' || '\n'
            || '1' || '\n'
            || '5' || '\n'
            || '5' || '\n'
            || '7' || '\n'
            || '6' || '\n'
            || '5' || '\n'
            || '4' || '\n'
            || '3' || '\n'
            || '1' || '\n'
            || '-++-++-++') as t (value)
)
select aggregate( sequence(0, array_size(signs) - 1)
                , magnitudes[0]::int
                , (acc, x) -> acc + magnitudes[x+1]::int * case signs[x] when '+' then 1 when '-' then -1 end
                ) as part1
     , aggregate( sequence(0, array_size(signs) - 1)
                , magnitudes[0]::int
                , (acc, x) -> acc + magnitudes[x+1]::int * case reverse(signs)[x] when '+' then 1 when '-' then -1 end
                ) as part2
     , aggregate( sequence(0, array_size(signs) div 2 - 1)
                , magnitudes[0]::int * 10 + magnitudes[1]::int
                , (acc, x) -> acc + (magnitudes[2*x+2]::int * 10 + magnitudes[2*x+3]::int) * case reverse(signs)[x] when '+' then 1 when '-' then -1 end
                ) as part3
  from cte_data;
