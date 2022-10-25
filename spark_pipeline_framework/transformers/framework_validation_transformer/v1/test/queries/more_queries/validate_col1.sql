select
    case
        when col1_null_count > 0 THEN 1
        ELSE 0
    END as is_failed,
    CONCAT('Column1 has ', col1_null_count, ' null value(s)') as message
FROM
(select count(*) as col1_null_count
from my_view
where Column1 is null or Column1 = '')