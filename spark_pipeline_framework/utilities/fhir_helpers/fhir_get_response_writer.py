from typing import List, Optional, Any, Dict
from pyspark.sql.types import Row


class FhirGetResponseWriter:
    @staticmethod
    def create_row(
        partition_index: int,
        sent: int,
        received: int,
        responses: List[str],
        first: Optional[str],
        last: Optional[str],
        error_text: Optional[str],
        url: Optional[str],
        status_code: int,
        request_id: Optional[str],
        extra_context_to_return: Optional[Dict[str, Any]],
        access_token: Optional[str],
    ) -> Row:
        return Row(
            partition_index=partition_index,
            sent=sent,
            received=received,
            responses=responses,
            first=first,
            last=last,
            error_text=error_text,
            url=url,
            status_code=status_code,
            request_id=request_id,
            extra_context_to_return=extra_context_to_return,
            access_token=access_token,
        )
