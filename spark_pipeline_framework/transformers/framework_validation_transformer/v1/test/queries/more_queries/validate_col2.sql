select
    case
        when col2_null_count > 0 THEN 1
        ELSE 0
    END as is_failed,
    CONCAT('Column2 has ', col2_null_count, ' null value(s)') as message
FROM
(select count(*) as col2_null_count
from my_view
where Column2 is null or Column2 = '')