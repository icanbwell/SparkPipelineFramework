select
    case
        when flight_count = 300 THEN 0
        ELSE 1
    END as is_failed,
    CONCAT('Expected 300 flights has ', flight_count, ' flights') as message
FROM
(select count(*) as flight_count
from flights)