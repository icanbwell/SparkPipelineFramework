from typing import Optional, cast

import requests
from requests.auth import HTTPBasicAuth


class TokenHelper:
    @staticmethod
    def get_oauth_token(
        *, client_id: str, client_secret: str, token_url: str, scope: Optional[str]
    ) -> Optional[str]:
        # Prepare the headers and body for the request
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}
        if scope:
            data["scope"] = scope

        # Make the POST request to the token endpoint
        response = requests.post(
            token_url,
            headers=headers,
            data=data,
            auth=HTTPBasicAuth(client_id, client_secret),
        )

        # Check if the request was successful
        if response.status_code == 200:
            token_data = response.json()
            return cast(Optional[str], token_data.get("access_token"))
        else:
            raise Exception(
                f"Failed to get token: {response.status_code}, {response.text}"
            )
