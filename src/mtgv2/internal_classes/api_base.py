class APIClient:
    def __init__(self, uri: str, token=None):
        self.uri = uri
        self.token = token

    def fetch(self, **kwargs):
        # To be overritten via individual calls
        raise NotImplementedError("Call must contain fetch.")

    def get_headers(self):
        headers = {}

        # Check for headers
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        return headers
