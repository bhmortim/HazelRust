#!/usr/bin/env python3
"""Pure-stdlib HdrHistogram V2 (+ zlib-compressed) decoder.

Both harnesses emit the standard HdrHistogram V2 compressed interchange format
(Rust `V2DeflateSerializer`, Java `encodeIntoCompressedByteBuffer`). This module
decodes either into a counts array so the analyzer can (a) verify cross-language
compatibility against the harness-reported percentiles and (b) MERGE histograms
across forks for pooled percentiles and CDFs — with zero third-party deps.

Format (big-endian):
  compressed: int cookie=0x1c849304, int len, <zlib stream>
  inflated  : int cookie=0x1c849303, int payloadLen, int normIdxOffset,
              int sigFigs, long lowest, long highest, double convRatio,
              <payloadLen bytes of ZigZag-LEB128 counts (negative => zero-run)>
"""
import base64
import struct
import zlib
import math

V2_COOKIE = 0x1C849303
V2_COMPRESSED_COOKIE = 0x1C849304


class Hist:
    def __init__(self, lowest, highest, sigfigs):
        self.lowest = lowest
        self.highest = highest
        self.sigfigs = sigfigs
        largest = 2 * (10 ** sigfigs)
        self.sub_bucket_count_mag = int(math.ceil(math.log2(largest)))
        self.sub_bucket_half_count_mag = self.sub_bucket_count_mag - 1
        self.sub_bucket_count = 1 << (self.sub_bucket_half_count_mag + 1)
        self.sub_bucket_half_count = self.sub_bucket_count >> 1
        self.unit_mag = int(math.floor(math.log2(lowest))) if lowest >= 1 else 0
        self.counts = {}   # index -> count
        self.total = 0

    def _decode_index(self, index):
        bucket_index = (index >> self.sub_bucket_half_count_mag) - 1
        sub_bucket_index = (index & (self.sub_bucket_half_count - 1)) + self.sub_bucket_half_count
        if bucket_index < 0:
            sub_bucket_index -= self.sub_bucket_half_count
            bucket_index = 0
        shift = bucket_index + self.unit_mag
        value = sub_bucket_index << shift
        size = 1 << shift
        return value, size

    def add_count(self, index, count):
        if count <= 0:
            return
        self.counts[index] = self.counts.get(index, 0) + count
        self.total += count

    def merge(self, other):
        # both share identical (lowest, highest, sigfigs) => identical layout
        for idx, c in other.counts.items():
            self.counts[idx] = self.counts.get(idx, 0) + c
        self.total += other.total
        return self

    def value_at_percentile(self, p):
        if self.total == 0:
            return 0
        target = int(math.ceil((p / 100.0) * self.total))
        if target < 1:
            target = 1
        acc = 0
        for idx in sorted(self.counts):
            acc += self.counts[idx]
            if acc >= target:
                value, size = self._decode_index(idx)
                return value + size - 1   # highest-equivalent value
        # fallback: max
        last = max(self.counts)
        v, s = self._decode_index(last)
        return v + s - 1

    def max_value(self):
        if not self.counts:
            return 0
        v, s = self._decode_index(max(self.counts))
        return v + s - 1

    def min_value(self):
        if not self.counts:
            return 0
        v, _ = self._decode_index(min(self.counts))
        return v

    def mean(self):
        if self.total == 0:
            return 0.0
        tot = 0.0
        for idx, c in self.counts.items():
            v, s = self._decode_index(idx)
            mid = v + (s >> 1)
            tot += mid * c
        return tot / self.total

    def cdf_points(self, n=400):
        """Return [(value, quantile)] sampled across the distribution for CDF plots."""
        if self.total == 0:
            return []
        pts = []
        acc = 0
        items = sorted(self.counts.items())
        # produce a point at each populated bucket (capped to n by stride)
        stride = max(1, len(items) // n)
        for i, (idx, c) in enumerate(items):
            acc += c
            if i % stride == 0 or i == len(items) - 1:
                v, s = self._decode_index(idx)
                pts.append((v + s - 1, acc / self.total))
        return pts


def _zigzag_decode_stream(buf):
    """Yield decoded longs from a ZigZag-LEB128 byte buffer."""
    i = 0
    n = len(buf)
    while i < n:
        value = 0
        shift = 0
        while True:
            b = buf[i]
            i += 1
            value |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        # zigzag decode
        decoded = (value >> 1) ^ (-(value & 1))
        yield decoded


def _family_ok(cookie):
    # The HdrHistogram cookie family: high 24 bits == 0x1c8493. The low byte
    # encodes version/word-size and differs between the Java and Rust libraries
    # (Java compressed=0x..04, Rust compressed=0x..14) — both are valid V2.
    return ((cookie >> 8) & 0xFFFFFF) == 0x1C8493


def decode_compressed(b64):
    raw = base64.b64decode(b64)
    (cookie,) = struct.unpack_from(">i", raw, 0)
    if not _family_ok(cookie):
        raise ValueError("not a HdrHistogram cookie (0x%x)" % (cookie & 0xFFFFFFFF))
    # The zlib stream follows the cookie (+ optional 4-byte length). Locate it by
    # its header byte (0x78) so we are robust to the length-field convention.
    inflated = None
    for off in (8, 4):
        if off < len(raw) and raw[off] == 0x78:
            try:
                inflated = zlib.decompress(raw[off:])
                break
            except zlib.error:
                pass
    if inflated is None:
        inflated = zlib.decompress(raw[8:])  # last resort
    return decode_v2(inflated)


def decode_v2(data):
    cookie, payload_len, norm_off, sigfigs = struct.unpack_from(">iiii", data, 0)
    lowest, highest = struct.unpack_from(">qq", data, 16)
    (conv_ratio,) = struct.unpack_from(">d", data, 32)
    if not _family_ok(cookie):
        raise ValueError("not a V2 histogram (cookie=0x%x)" % (cookie & 0xFFFFFFFF))
    payload = data[40:40 + payload_len]
    h = Hist(lowest, highest, sigfigs)
    idx = 0
    for v in _zigzag_decode_stream(payload):
        if v < 0:
            idx += -v   # zero-run
        else:
            h.add_count(idx, v)
            idx += 1
    return h


if __name__ == "__main__":
    # Self-validation: decode a record's hgrm_b64 and compare to its reported
    # percentiles. Usage: python3 bench/hdr.py <record.json>
    import json
    import sys
    rec = json.load(open(sys.argv[1]))
    lat = rec["latency"]
    h = decode_compressed(lat["hgrm_b64"])
    print("file:", sys.argv[1])
    print("  client:", rec.get("client"), "cell:", rec.get("cell_id"))
    print("  decoded total =", h.total, " reported count =", lat["count"])
    for p, key in [(50, "p50"), (99, "p99"), (99.9, "p999"), (99.99, "p9999")]:
        dv = h.value_at_percentile(p) / 1000.0  # ns -> us
        print("  p%-5s decoded=%.3fus reported=%.3fus  (delta %.2f%%)"
              % (p, dv, lat[key], (abs(dv - lat[key]) / max(lat[key], 1e-9)) * 100))
