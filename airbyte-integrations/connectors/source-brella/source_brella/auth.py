from airbyte_cdk.sources.streams.http.requests_native_auth.abstract_token import AbstractHeaderAuthenticator

class BrellaAuthenticator(AbstractHeaderAuthenticator):
    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        return self._token

    def __init__(self, token: str, auth_header: str = "Brella-API-Access-Token"):
        self._auth_header = auth_header
        self._token = token 
