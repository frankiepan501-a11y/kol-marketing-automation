import json
import tempfile
import unittest
from pathlib import Path

from scripts.refresh_kol_profile_quality import (
    _normalized,
    selected_ids as profile_ids,
)
from scripts.repair_kol_email_quality import selected_ids as email_ids


class KolP0RepairScriptTests(unittest.TestCase):
    def _cohorts(self):
        temp = tempfile.NamedTemporaryFile(
            suffix=".json", mode="w", encoding="utf-8", delete=False,
        )
        json.dump({"campaigns": {
            "食人花": {
                "email": [{"record_id": "p-email"}],
                "profile_refresh": [
                    {"record_id": "p-youtube", "platform": "YouTube"},
                    {"record_id": "p-instagram", "platform": "Instagram"},
                ],
            },
            "Dave": {"email": [{"record_id": "d-email"}]},
        }}, temp, ensure_ascii=False)
        temp.close()
        self.addCleanup(lambda: Path(temp.name).unlink(missing_ok=True))
        return temp.name

    def test_profile_selection_never_sends_instagram_to_youtube_refresher(self):
        selected, unsupported = profile_ids(self._cohorts())
        self.assertEqual(["p-youtube"], selected)
        self.assertEqual(
            [{"record_id": "p-instagram", "platform": "Instagram"}], unsupported,
        )

    def test_email_selection_prioritizes_small_piranha_cohort(self):
        self.assertEqual(
            ["p-email", "d-email"], email_ids(self._cohorts(), limit=2),
        )

    def test_profile_readback_normalizes_bitable_text_and_multiselect_shapes(self):
        self.assertEqual(
            _normalized("first\nsecond"),
            _normalized([{"text": "first\n"}, {"text": "second"}]),
        )
        self.assertEqual(
            _normalized(["Switch", "PC"]),
            _normalized(["PC", "Switch"]),
        )
        self.assertEqual(_normalized(2), _normalized("2"))


if __name__ == "__main__":
    unittest.main()
