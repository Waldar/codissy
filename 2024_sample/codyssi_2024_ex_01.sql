with cte_data (id, value) as
(
select monotonically_increasing_id()
     , value::int
--from read_files('/Volumes/waldar/fabien/codyssi/2024_sample/codyssi_2024_01.txt', format => 'text')
  from values ('912372')
            , ('283723')
            , ('294281')
            , ('592382')
            , ('721395')
            , ('91238' ) as t (value)
)
  ,  cte_data_prep (id, rn, lim, value) as
(
 select row_number() over wi
      , row_number() over w2
      , case count(*) over wl when 6 then 2 else 20 end
      , value
   from cte_data
 window wi as (order by id asc)
      , w2 as (order by value::int desc)
      , wl as ()
)
select sum(value)                                           as part1
     , sum(value) filter(where rn > lim)                    as part2
     , sum(case when id % 2 = 1 then 1 else -1 end * value) as part3
  from cte_data_prep;
