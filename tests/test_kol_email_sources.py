import unittest
from unittest.mock import AsyncMock, patch

from app import kol_email_sources


class KolEmailSourcesTests(unittest.TestCase):
    def test_public_source_url_classification_separates_aggregator_and_website(self):
        self.assertEqual(
            "aggregate",
            kol_email_sources.classify_public_source_url("https://beacons.ai/creator"),
        )
        self.assertEqual(
            "website",
            kol_email_sources.classify_public_source_url("https://creator.example/"),
        )
        self.assertEqual(
            "",
            kol_email_sources.classify_public_source_url(
                "https://www.instagram.com/creator/",
            ),
        )
        self.assertEqual("", kol_email_sources.classify_public_source_url(
            "http://127.0.0.1/internal",
        ))
        self.assertEqual("", kol_email_sources.classify_public_source_url(
            "http://192.168.1.8/private",
        ))
        self.assertEqual("", kol_email_sources.classify_public_source_url(
            "http://localhost/admin",
        ))

    def test_extract_public_emails_reads_visible_and_mailto_and_deduplicates(self):
        html = '''
        <a href="mailto:Business@Creator.com?subject=Collab">Email us</a>
        <p>Business: business@creator.com</p>
        <p>Template: creator@example.com</p>
        '''

        self.assertEqual(
            ["business@creator.com"],
            kol_email_sources.extract_public_emails(html),
        )

    def test_source_urls_use_only_explicit_master_links(self):
        fields = {
            "聚合页URL": {"link": "https://linktr.ee/creator"},
            "其他链接": "Website https://creator.example/contact\nnot-a-url",
            "主链接": {"link": "https://www.youtube.com/@creator"},
        }

        self.assertEqual([
            {"url": "https://linktr.ee/creator", "source": "master_aggregate"},
            {"url": "https://creator.example/contact", "source": "master_other"},
        ], kol_email_sources.master_source_urls(fields))

    def test_contact_links_prioritize_same_site_contact_pages(self):
        html = '''
        <a href="/videos">Videos</a>
        <a href="/about-us">About</a>
        <a href="https://other.example/contact">Other</a>
        <a href="/contact">Business Contact</a>
        '''

        self.assertEqual([
            "https://creator.example/contact",
            "https://creator.example/about-us",
        ], kol_email_sources.contact_page_urls(
            html, "https://creator.example/home", limit=2,
        ))

    def test_aggregator_external_links_ignore_stylesheet_and_asset_hrefs(self):
        html = '''
        <link rel="stylesheet" href="https://cdn.assets.example/site.css">
        <a href="https://creator.example/">Official site</a>
        '''

        self.assertEqual(
            ["https://creator.example/"],
            kol_email_sources._aggregator_external_urls(
                html, "https://linktr.ee/creator", limit=2,
            ),
        )

    def test_generic_homepage_is_not_contact_evidence(self):
        self.assertFalse(kol_email_sources._is_contact_evidence_page(
            "https://creator.example/", "youtube_external",
        ))
        self.assertTrue(kol_email_sources._is_contact_evidence_page(
            "https://creator.example/contact", "youtube_external_contact",
        ))
        self.assertTrue(kol_email_sources._is_contact_evidence_page(
            "https://linktr.ee/creator", "master_aggregate",
        ))

    def test_common_link_in_bio_hosts_are_explicitly_allowlisted(self):
        for url in (
            "https://beacons.ai/creator",
            "https://bio.site/creator",
            "https://creator.carrd.co/",
            "https://hoo.be/creator",
            "https://stan.store/creator",
        ):
            with self.subTest(url=url):
                self.assertTrue(kol_email_sources._is_aggregator(url))

    def test_instagram_parser_reads_only_explicit_bio_links(self):
        source = r'''
        <script src="https://assets.production.linktr.ee/runtime.js"></script>
        <script type="application/json">
          {"user":{"bio_links":[
            {"title":"My links","url":"https:\/\/beacons.ai\/creator"}
          ]}}
        </script>
        '''

        self.assertEqual(
            ["https://beacons.ai/creator"],
            kol_email_sources.social_profile_external_urls(
                source, "https://www.instagram.com/creator/",
            ),
        )

    def test_tiktok_parser_reads_only_explicit_bio_link(self):
        source = r'''
        <script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">
          {"userInfo":{"user":{"bioLink":{"link":"https://linktr.ee/creator"}}}}
        </script>
        '''

        self.assertEqual(
            ["https://linktr.ee/creator"],
            kol_email_sources.social_profile_external_urls(
                source, "https://www.tiktok.com/@creator",
            ),
        )

    def test_social_parser_ignores_marker_without_json_value(self):
        source = r'''
        "bio_links" rendered-label-only
        {"asset":{"url":"https://linktr.ee/not-a-profile-link"}}
        {"suggested_user":{"external_url":"https://beacons.ai/not-this-profile"}}
        '''

        self.assertEqual(
            [],
            kol_email_sources.social_profile_external_urls(
                source, "https://www.instagram.com/creator/",
            ),
        )


class KolEmailSourcesNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_youtube_external_links_return_public_landing_page_candidates(self):
        profile = {
            "emails": [],
            "external_links": [
                {"url": "https://linktr.ee/creator"},
                {"url": "https://creator.example/contact"},
                {"url": "https://www.instagram.com/creator/"},
            ],
        }
        aggregator_page = {
            "ok": True,
            "url": "https://linktr.ee/creator",
            "text": '<a href="https://creator.example/">Official website</a>',
        }
        with patch.object(
            kol_email_sources.relabel, "fetch_youtube_public_profile",
            new=AsyncMock(return_value=profile),
        ), patch.object(
            kol_email_sources, "fetch_public_page",
            new=AsyncMock(return_value=aggregator_page),
        ):
            result = await kol_email_sources.discover_public_landing_page_candidates({
                "主链接": {"link": "https://www.youtube.com/@creator"},
                "聚合页URL": "",
                "其他链接": "",
            })

        self.assertEqual([
            {
                "url": "https://linktr.ee/creator",
                "kind": "aggregate",
                "source": "youtube_external",
            },
            {
                "url": "https://creator.example/contact",
                "kind": "website",
                "source": "youtube_external",
            },
            {
                "url": "https://creator.example/",
                "kind": "website",
                "source": "youtube_external_linked",
            },
        ], result)

    async def test_instagram_explicit_bio_link_returns_aggregate_candidate(self):
        social_page = {
            "ok": True,
            "url": "https://www.instagram.com/creator/",
            "text": r'{"bio_links":[{"url":"https:\/\/beacons.ai\/creator"}]}',
        }
        aggregator_page = {
            "ok": True,
            "url": "https://beacons.ai/creator",
            "text": "<html></html>",
        }
        with patch.object(
            kol_email_sources, "fetch_public_page",
            new=AsyncMock(side_effect=[social_page, aggregator_page]),
        ):
            result = await kol_email_sources.discover_public_landing_page_candidates({
                "主链接": {"link": "https://www.instagram.com/creator/"},
            })

        self.assertEqual([{
            "url": "https://beacons.ai/creator",
            "kind": "aggregate",
            "source": "instagram_external",
        }], result)

    @patch("app.kol_email_sources.socket.getaddrinfo")
    async def test_proxy_fake_dns_is_allowed_only_when_explicitly_requested(self, getaddrinfo):
        getaddrinfo.return_value = [(2, 1, 6, "", ("198.18.1.95", 0))]

        self.assertFalse(await kol_email_sources._public_host("linktr.ee"))
        self.assertTrue(await kol_email_sources._public_host(
            "linktr.ee", allow_proxy_fake_dns=True,
        ))

    async def test_instagram_bio_link_flows_to_public_email_discovery(self):
        social_page = {
            "ok": True,
            "url": "https://www.instagram.com/creator/",
            "text": r'{"bio_links":[{"url":"https:\/\/beacons.ai\/creator"}]}',
        }
        landing_page = {
            "ok": True,
            "url": "https://beacons.ai/creator",
            "text": '<a href="mailto:business@creator.test">Business</a>',
        }
        fetch = AsyncMock(side_effect=[social_page, landing_page])
        with patch.object(kol_email_sources, "fetch_public_page", new=fetch):
            result = await kol_email_sources.discover_public_email_candidates({
                "主链接": {"link": "https://www.instagram.com/creator/"},
                "聚合页URL": "",
                "其他链接": "",
            })

        self.assertEqual([{
            "email": "business@creator.test",
            "source": "instagram_external",
            "source_url": "https://beacons.ai/creator",
        }], result)
        self.assertEqual(2, fetch.await_count)

    async def test_primary_link_in_bio_is_treated_as_public_source(self):
        landing_page = {
            "ok": True,
            "url": "https://linktr.ee/creator",
            "text": '<a href="mailto:collab@creator.test">Collab</a>',
        }
        fetch = AsyncMock(return_value=landing_page)
        with patch.object(kol_email_sources, "fetch_public_page", new=fetch):
            result = await kol_email_sources.discover_public_email_candidates({
                "主链接": {"link": "https://linktr.ee/creator"},
                "聚合页URL": "",
                "其他链接": "",
            })

        self.assertEqual([{
            "email": "collab@creator.test",
            "source": "primary_aggregate",
            "source_url": "https://linktr.ee/creator",
        }], result)
        fetch.assert_awaited_once_with("https://linktr.ee/creator")


if __name__ == "__main__":
    unittest.main()
