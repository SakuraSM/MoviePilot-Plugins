from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.grpc_web import (
    FrameDecoder,
    GrpcWebError,
    frame_message,
    parse_trailers,
    raise_for_grpc_status,
)


class GrpcWebTests(unittest.TestCase):
    def test_decoder_accepts_arbitrary_chunk_boundaries(self) -> None:
        encoded = frame_message(b"first") + frame_message(b"second")
        decoder = FrameDecoder()
        frames = []
        for byte in encoded:
            frames.extend(decoder.feed(bytes([byte])))
        decoder.finish()
        self.assertEqual([frame.payload for frame in frames], [b"first", b"second"])

    def test_decoder_rejects_truncated_frame(self) -> None:
        decoder = FrameDecoder()
        decoder.feed(frame_message(b"abc")[:-1])
        with self.assertRaises(GrpcWebError):
            decoder.finish()

    def test_grpc_status_is_decoded(self) -> None:
        trailers = parse_trailers(b"grpc-status: 7\r\ngrpc-message: permission%20denied\r\n")
        with self.assertRaisesRegex(GrpcWebError, "permission denied"):
            raise_for_grpc_status(trailers)
