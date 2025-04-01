import json
from typing import Optional, Dict, Any, Union, List


class FhirReceiverException(Exception):
    def __init__(
        self,
        *,
        url: str,
        json_data: str,
        response_text: Optional[str],
        response_status_code: Optional[int],
        message: str,
        request_id: Optional[str],
    ) -> None:
        # Try to parse the json_data if possible
        parsed_json_data: Union[str, Dict[str, Any], List[Dict[str, Any]]] = json_data
        try:
            # Attempt to parse the json_data string into a dictionary
            parsed_json_data = json.loads(json_data)
        except (json.JSONDecodeError, TypeError):
            # If parsing fails, keep the original string
            pass

        # Prepare the error context dictionary
        error_context: Dict[str, Any] = {
            "message": f"FHIR receive failed: {message}",
            "url": url,
            "status_code": response_status_code,
            "response_text": response_text,
            # "json_data": parsed_json_data,
            "request_id": request_id,
        }

        # Store attributes for potential later access
        self.url: str = url
        self.data: Union[str, Dict[str, Any], List[Dict[str, Any]]] = parsed_json_data
        self.error_context: Dict[str, Any] = error_context

        # Custom string representation to improve readability
        error_message = self._format_error_message(error_context)

        # Call parent constructor with custom formatted message
        super().__init__(error_message)

    @staticmethod
    def _format_error_message(error_context: Dict[str, Any]) -> str:
        """
        Create a more readable error message with pretty-printed JSON if possible.

        Args:
            error_context (Dict[str, Any]): The error context dictionary

        Returns:
            str: Formatted error message
        """
        # Create a copy to avoid modifying the original context
        formatted_context = error_context.copy()

        # Try to pretty print JSON data if it's a dictionary
        # if isinstance(formatted_context['json_data'], dict) or isinstance(formatted_context['json_data'], list):
        #     try:
        #         # Use json.dumps with indentation for readability
        #         formatted_context['json_data'] = json.dumps(
        #             formatted_context['json_data'],
        #             indent=2,
        #             ensure_ascii=False
        #         )
        #     except Exception:
        #         # Fallback to str if JSON dumping fails
        #         formatted_context['json_data'] = str(formatted_context['json_data'])

        # Convert to a readable string format
        return "\n".join(
            [
                f"{key.replace('_', ' ').title()}\n{value}"
                for key, value in formatted_context.items()
                if value is not None
            ]
        )

    def __str__(self) -> str:
        """
        Override the default string representation to use our custom formatting.

        Returns:
            str: Formatted error message
        """
        return self._format_error_message(self.error_context)
