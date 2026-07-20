import unittest

from app import cs_resources


UPGRADE_HTML = """
<table>
  <tr><td>Firefly</td><td>FF01</td><td>V180/V191/V199</td><td><a href="https://drive.google.com/drive/folders/firefly203">Download: V203</a></td></tr>
  <tr><td>Luminex</td><td>FF05</td><td>V412/V447/V453</td><td><a href="https://drive.google.com/drive/folders/12Pj09f83wBIdCce2hHEkhVdQuqxZYehW?usp=sharing">Download: V454</a></td></tr>
  <tr><td>Luminex</td><td>FF05</td><td>V417/V432/V444</td><td><a href="https://drive.google.com/drive/folders/1J_WkY5mKiUrttYUQFzNlpInJUsmBDMJZ?usp=sharing">Download: V459</a></td></tr>
</table>
"""

MANUAL_HTML = """
<a href="https://drive.google.com/drive/folders/10ZQLVbKGgmzGP-BP7kvtUlf9HN3g-rnN?usp=sharing">English Version</a>
<a href="https://drive.google.com/drive/folders/1z5wfyPppT6x7qMH8bJGByD82K970aBhb?usp=sharing">Chinese Version</a>
"""

HOW_TO_HTML = """
<h2>FUNLAB Luminex Video Tutorial</h2>
<script id="Video-data-1" type="application/json">{"video_url":"https://cdn-files.myshopline.com/luminex-connect.mp4","sub_title":"1. Connecting Controller Instruction"}</script>
<h2>FUNLAB Firefly Video Tutorial</h2>
<script id="Video-data-2" type="application/json">{"video_url":"https://cdn-files.myshopline.com/firefly-turbo.mp4","sub_title":"2. Turbo Function Instruction"}</script>
"""


def ff05_fields(version: str = "") -> dict:
    version_text = f" Current version {version}." if version else ""
    return {
        "品牌": "FUNLAB",
        "产品": "FF05A controller",
        "客户标识": "Santiago.guzmanp@hotmail.com",
        "客诉摘要": "The controller is not vibrating after reset and firmware update.",
        "原文": "FF05A controller vibration issue after firmware update." + version_text,
    }


class CSResourceParserTest(unittest.TestCase):
    def test_parse_official_resources(self):
        firmware = cs_resources.parse_upgrade_firmware_html(UPGRADE_HTML)
        urls = {r["url"] for r in firmware}
        self.assertIn("https://drive.google.com/drive/folders/12Pj09f83wBIdCce2hHEkhVdQuqxZYehW?usp=sharing", urls)
        self.assertIn("https://drive.google.com/drive/folders/1J_WkY5mKiUrttYUQFzNlpInJUsmBDMJZ?usp=sharing", urls)

        manuals = cs_resources.parse_firmware_manual_html(MANUAL_HTML)
        self.assertEqual({r["language"] for r in manuals}, {"EN", "ZH"})

        videos = cs_resources.parse_how_to_video_html(HOW_TO_HTML)
        luminex = [r for r in videos if r.get("series") == "Luminex" and r.get("resource_type") == "how_to_video"]
        self.assertEqual(len(luminex), 1)
        self.assertEqual(luminex[0]["url"], "https://cdn-files.myshopline.com/luminex-connect.mp4")


class CSResourceResolverTest(unittest.TestCase):
    def test_ff05_version_exact_match(self):
        v453 = cs_resources.resolve_for_ticket(ff05_fields("V453"))
        firmware = [r for r in v453["matches"] if r["resource_type"] == "firmware_download"]
        self.assertEqual([r["conditions"]["target_version"] for r in firmware], ["V454"])

        v432 = cs_resources.resolve_for_ticket(ff05_fields("V432"))
        firmware = [r for r in v432["matches"] if r["resource_type"] == "firmware_download"]
        self.assertEqual([r["conditions"]["target_version"] for r in firmware], ["V459"])

    def test_ff05_no_current_version_is_ambiguous(self):
        ctx = cs_resources.resolve_for_ticket(ff05_fields())
        firmware = [r for r in ctx["matches"] if r["resource_type"] == "firmware_download"]
        self.assertEqual(ctx["status"], "有歧义")
        self.assertEqual({r["conditions"]["target_version"] for r in firmware}, {"V454", "V459"})

    def test_firefly_playlist_does_not_match_luminex(self):
        firefly = cs_resources.resolve_for_ticket({
            "品牌": "FUNLAB",
            "产品": "Firefly controller",
            "客诉摘要": "How to connect and use turbo.",
            "原文": "Please send how to video for Firefly.",
        })
        firefly_urls = {r["url"] for r in firefly["matches"]}
        self.assertIn(cs_resources.FIREFLY_YOUTUBE_PLAYLIST, firefly_urls)

        luminex = cs_resources.resolve_for_ticket({
            "品牌": "FUNLAB",
            "产品": "FF05A Luminex controller",
            "客诉摘要": "How to connect and use turbo.",
            "原文": "Please send how to video for Luminex FF05A.",
        })
        luminex_urls = {r["url"] for r in luminex["matches"]}
        self.assertNotIn(cs_resources.FIREFLY_YOUTUBE_PLAYLIST, luminex_urls)


class CSResourceSafetyTest(unittest.TestCase):
    def test_send_gate_blocks_false_attachment_and_placeholder_link(self):
        fields = ff05_fields()
        self.assertIn("附件", cs_resources.validate_reply_for_ticket(
            "Please find the firmware file attached to this email.",
            fields,
        ))
        self.assertIn("占位链接", cs_resources.validate_reply_for_ticket(
            "Download and run the update tool from our website: [link]",
            fields,
        ))

    def test_send_gate_requires_official_firmware_urls(self):
        fields = ff05_fields("V453")
        reason = cs_resources.validate_reply_for_ticket(
            "Please update the firmware to solve the vibration issue.",
            fields,
        )
        self.assertIn("官方固件 URL", reason)

    def test_resource_reply_is_safe_for_current_ff05_ticket(self):
        fields = ff05_fields()
        ctx = cs_resources.resolve_for_ticket(fields)
        reply = cs_resources.build_resource_reply(fields, ctx)
        self.assertIn("12Pj09f83wBIdCce2hHEkhVdQuqxZYehW", reply)
        self.assertIn("1J_WkY5mKiUrttYUQFzNlpInJUsmBDMJZ", reply)
        self.assertNotIn("[link]", reply)
        self.assertNotIn("attached", reply.lower())
        self.assertEqual("", cs_resources.validate_reply_for_ticket(reply, fields))

    def test_firefly_firmware_subset_of_official_urls_is_safe(self):
        fields = {
            "品牌": "FUNLAB",
            "产品": "FF01 Firefly controller",
            "客诉摘要": "After firmware update, ZR/ZL/ABXY are unresponsive and need firmware.",
            "原文": "FF01 firmware problem after update.",
        }
        resources = [
            cs_resources._resource(
                series="Firefly", model="FF01", resource_type="firmware_download",
                title="FUNLAB Firefly FF01 firmware V203",
                url="https://drive.google.com/drive/folders/1bW6cBkNNEkZftiPU_-KtOLHIc1z1p4E?usp=sharing",
                conditions={"target_version": "V203"}, source_page=cs_resources.UPGRADE_FIRMWARE_URL,
                issue_tags=["firmware"],
            ),
            cs_resources._resource(
                series="Firefly", model="FF01", resource_type="firmware_download",
                title="FUNLAB Firefly FF01 firmware V204",
                url="https://drive.google.com/drive/folders/1XwvhMGHsWhQ7tCBXd84zugJ59pZL9w62?usp=sharing",
                conditions={"target_version": "V204"}, source_page=cs_resources.UPGRADE_FIRMWARE_URL,
                issue_tags=["firmware"],
            ),
            cs_resources._resource(
                series="Firefly", model="FF01", resource_type="firmware_download",
                title="FUNLAB Firefly FF01 firmware V198",
                url="https://drive.google.com/drive/folders/1fPc-vOOiBSXO5neQlT8R5aUdnJ74XMiy?usp=sharing",
                conditions={"target_version": "V198"}, source_page=cs_resources.UPGRADE_FIRMWARE_URL,
                issue_tags=["firmware"],
            ),
            *cs_resources.builtin_resources(),
        ]
        reply = (
            "Hi, please use the official firmware links below.\n"
            "Download V203: https://drive.google.com/drive/folders/1bW6cBkNNEkZftiPU_-KtOLHIc1z1p4E\n"
            "Download V204: https://drive.google.com/drive/folders/1XwvhMGHsWhQ7tCBXd84zugJ59pZL9w62?usp=sharing\n"
        )

        self.assertEqual("", cs_resources.validate_reply_for_ticket(reply, fields, resources=resources))

    def test_ff05_ambiguous_firmware_still_requires_all_candidate_urls(self):
        fields = ff05_fields()
        reply = "Download V454: https://drive.google.com/drive/folders/12Pj09f83wBIdCce2hHEkhVdQuqxZYehW"

        reason = cs_resources.validate_reply_for_ticket(reply, fields)

        self.assertIn("多个候选固件", reason)


if __name__ == "__main__":
    unittest.main()
