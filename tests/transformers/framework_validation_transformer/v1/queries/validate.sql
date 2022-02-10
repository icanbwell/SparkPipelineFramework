select
    case
        when col3_null_count > 0 THEN 1
        ELSE 0
    END as is_failed,
    CONCAT('Column3 has ', col3_null_count, ' null value(s)') as message
FROM
(select count(*) as col3_null_count
from my_view
where Column3 is null or Column3 = '')