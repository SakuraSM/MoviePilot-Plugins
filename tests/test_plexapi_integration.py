from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

import _bootstrap  # noqa: F401
from clouddriveplexsync.activity_log import ActivityJournal
from clouddriveplexsync.content_verifier import ContentVerifier
from clouddriveplexsync.plex_bridge import PlexBridge
from clouddriveplexsync.verification_store import MediaVerification
from test_plex_bridge import FakePlex, FakeService

try:
    from plexapi.library import ShowSection
    from plexapi.video import Episode  # noqa: F401 - registers the XML object type.
except ImportError:
    ShowSection = None

VIDEO = """<Video type="episode" ratingKey="23" key="/library/metadata/23"
parentIndex="1" index="2" grandparentTitle="节目" title="第2集">
<Media id="4"><Part id="5" file="/plex/节目/Season 1/E02.mkv" exists="1" accessible="1" /></Media></Video>"""
FILTER_METADATA = """<MediaContainer><Meta>
<Type type="show" key="/library/sections/1/all?type=2" title="Shows">
  <Field key="show.title" title="Title" type="string"/>
</Type>
<Type type="season" key="/library/sections/1/all?type=3" title="Seasons"/>
<Type type="episode" key="/library/sections/1/all?type=4" title="Episodes"/>
<FieldType type="string"><Operator key="=" title="contains"/></FieldType>
<FieldType type="integer"><Operator key="=" title="is"/></FieldType>
</Meta></MediaContainer>"""


class PlexServerStub:
    version = "1.43.5.11079"
    platform = "Linux"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def query(self, key: str, **_kwargs: object) -> ElementTree.Element:
        self.calls.append(key)
        if "includeAdvanced=1" in key:
            return ElementTree.fromstring(FILTER_METADATA)
        return ElementTree.fromstring(
            f'<MediaContainer size="1" totalSize="1">{VIDEO}</MediaContainer>'
        )


@unittest.skipUnless(ShowSection, "PlexAPI is supplied by the MoviePilot runtime")
class PlexAPIIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_episode_lookup_uses_server_filters_and_exact_plex_part_path(
        self,
    ) -> None:
        server = PlexServerStub()
        section = ShowSection(
            server,
            ElementTree.fromstring(
                '<Directory type="show" key="1" title="动漫"><Location path="/plex"/></Directory>'
            ),
            initpath="/library/sections",
        )
        plex = FakePlex([section])
        plex.platform = "Linux"
        entry = MediaVerification(
            1,
            "1",
            "动漫",
            "/plex/节目/Season 1/E02.mkv",
            "节目",
            "TV",
            1,
            2,
            100,
            None,
            0,
            "",
        )
        with tempfile.TemporaryDirectory() as directory:
            journal = await ActivityJournal.open(Path(directory) / "activity.sqlite3")
            try:
                verifier = ContentVerifier(
                    PlexBridge(FakeService(plex), ["1"]), journal
                )
                self.assertTrue(verifier._is_indexed(entry))
                self.assertTrue(
                    any(
                        "show.title=" in call
                        and "season.index=1" in call
                        and "episode.index=2" in call
                        for call in server.calls
                    )
                )
                self.assertFalse(any("allLeaves" in call for call in server.calls))
            finally:
                await journal.close()
