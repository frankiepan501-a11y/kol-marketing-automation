import io
import json
import unittest
import urllib.error
from unittest.mock import patch

from app.clients import ApiError, _json_request


class JsonRequestErrorInfoTests(unittest.TestCase):
    def test_google_error_info_is_parsed_into_structured_fields(self):
        payload = json.dumps({
            "error": {
                "code": 403,
                "message": "Permission denied for secret-project",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "IAM_PERMISSION_DENIED",
                    "domain": "iam.googleapis.com",
                    "metadata": {
                        "permission": "monitoring.timeSeries.list",
                        "resource": "projects/secret-project",
                    },
                }],
            }
        }).encode("utf-8")
        response = urllib.error.HTTPError(
            "https://example.invalid", 403, "Forbidden", {}, io.BytesIO(payload)
        )

        with patch("urllib.request.urlopen", side_effect=response):
            with self.assertRaises(ApiError) as caught:
                _json_request("GET", "https://example.invalid")

        self.assertEqual(caught.exception.code, "403")
        self.assertEqual(caught.exception.reason, "IAM_PERMISSION_DENIED")
        self.assertEqual(caught.exception.domain, "iam.googleapis.com")
        self.assertEqual(
            caught.exception.metadata["permission"], "monitoring.timeSeries.list"
        )

    def test_non_list_google_error_details_still_becomes_api_error(self):
        payload = json.dumps({
            "error": {
                "code": 403,
                "message": "Permission denied",
                "details": None,
            }
        }).encode("utf-8")
        response = urllib.error.HTTPError(
            "https://example.invalid", 403, "Forbidden", {}, io.BytesIO(payload)
        )

        with patch("urllib.request.urlopen", side_effect=response):
            with self.assertRaises(ApiError) as caught:
                _json_request("GET", "https://example.invalid")

        self.assertEqual(caught.exception.code, "403")
        self.assertEqual(caught.exception.reason, "")
        self.assertEqual(caught.exception.metadata, {})


if __name__ == "__main__":
    unittest.main()
