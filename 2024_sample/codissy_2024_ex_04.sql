with cte_data (source, dest) as
(
select left (value, 3)
     , right(value, 3)
--from read_files('/Volumes/waldar/fabien/codyssi2024/codyssi2024_04.txt', format => 'text')
  from values ('ADB <-> XYZ')
            , ('STT <-> NYC')
            , ('PLD <-> XYZ')
            , ('ADB <-> NYC')
            , ('JLI <-> NYC')
            , ('PTO <-> ADB') as t (value)
)
  ,  cte_data_prep1 (arr) as
(
select array(source, dest) from cte_data union all
select array(dest, source) from cte_data
)
  ,  cte_data_prep2 (arr) as
(
select array_agg(arr)
  from cte_data_prep1
)
select array_size(array_distinct(transform(arr, a -> a[0]))) as part1
     , aggregate( sequence(0, 2)
                , named_struct('done', array('STT'), 'path', array('STT'))
                , (acc, x) -> named_struct( 'done', acc.done || array_sort(array_distinct(transform(flatten(transform( filter(acc.path, p -> array_size(filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))) > 0)
                                                                                , p -> filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))))
                                                                     , u -> u[1])))
                                          , 'path', array_sort(array_distinct(transform(flatten(transform( filter(acc.path, p -> array_size(filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))) > 0)
                                                                                , p -> filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))))
                                                                     , u -> u[1])))
                                          )
                , acc -> array_size(acc.done)
                ) as part2
     , aggregate( sequence(0, 10)
                , named_struct('time', 0, 'done', array('STT'), 'path', array('STT'), 'last', 0)
                , (acc, x) -> case array_size(filter(acc.path, p -> array_size(filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))) > 0))
                                when 0
                                then acc
                                else named_struct( 'time', acc.time + x * array_size(array_distinct(acc.path))
                                                 , 'done', acc.done || array_sort(array_distinct(transform(flatten(transform( filter(acc.path, p -> array_size(filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))) > 0)
                                                                                       , p -> filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))))
                                                                            , u -> u[1])))
                                                 , 'path', array_sort(array_distinct(transform(flatten(transform( filter(acc.path, p -> array_size(filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))) > 0)
                                                                                       , p -> filter(arr, f -> f[0] = p and not array_contains(acc.done, f[1]))))
                                                                            , u -> u[1])))
                                                 , 'last', x
                                                 )
                              end
                , acc -> acc.time + (acc.last + 1) * array_size(acc.path)
                ) as part3
  from cte_data_prep2;

