with cte_data (id, value) as
(
select row_number() over W - 1
     , value::boolean
--from read_files('/Volumes/waldar/fabien/codyssi/2024_sample/codyssi_2024_02.txt', format => 'text')
  from values ('TRUE' )
            , ('FALSE')
            , ('TRUE' )
            , ('FALSE')
            , ('FALSE')
            , ('FALSE')
            , ('TRUE' )
            , ('TRUE' ) as t (value)
window W as (order by monotonically_increasing_id() asc)
)
select sum(id+1) filter(where value) as part1
     , array_size(filter(transform( sequence(0, shiftright(count(*), 1) - 1)
                                  , t -> case
                                           when t % 2 = 0
                                           then array_sort(array_agg(named_struct('id', id, 'val', value))).val[t*2] and array_sort(array_agg(named_struct('id', id, 'val', value))).val[t*2+1]
                                           else array_sort(array_agg(named_struct('id', id, 'val', value))).val[t*2]  or array_sort(array_agg(named_struct('id', id, 'val', value))).val[t*2+1]
                                          end
                                   ), v -> v)) as part2
     , aggregate( sequence(log(2, count(*))::int-1, 0, -1)
                , named_struct('s', 0, 'a', array_sort(array_agg(named_struct('id', id, 'val', value))))
                , (acc, x) -> named_struct( 's', acc.s + array_size(filter(acc.a.val, v -> v))
                                          , 'a', transform( sequence(0, power(2, x)::int - 1)
                                                          , t -> named_struct('id', t +1, 'val', case
                                                                                                   when t % 2 = 0
                                                                                                   then acc.a[t*2].val and acc.a[t*2+1].val
                                                                                                   else acc.a[t*2].val  or acc.a[t*2+1].val
                                                                                                 end) ) )
                , acc -> acc.s + array_size(filter(acc.a.val, v -> v))
                ) as part3
  from cte_data;
