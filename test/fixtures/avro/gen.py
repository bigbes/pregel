#!/usr/bin/env python3
"""Generate reference Avro fixtures with fastavro.

    uv run --with fastavro python3 test/fixtures/avro/gen.py

For every case below this writes, into the directory holding this script:

    <name>.avsc        the schema, as JSON
    <name>.json        the values, one JSON document per line
    <name>.bin         the plain binary encoding of those values, concatenated
    <name>.meta.json   the parsing canonical form and the CRC-64-AVRO
                       fingerprint of the schema
    <name>.null.avro   an object container file, codec "null"
    <name>.deflate.avro  the same records, codec "deflate"

The Lua test suite (test/unit/avro_interop_test.lua) decodes every one of these
and compares against the .json expectation, and re-encodes the expectation to
compare bytes with the .bin.

The .json files spell bytes and fixed values as {"$bytes": "<hex>"} so that a
byte string survives the JSON round trip on both sides -- JSON has no byte
type, and fastavro hands these back as Python bytes.

Alongside the Avro cases this also writes a raw-deflate corpus, which has
nothing to do with Avro's data model and everything to do with the pure-Lua
inflater that reads a deflate block on a build without compress.zlib:

    deflate_corpus.raw        the plaintext
    deflate_corpus.l{1,6,9}.z the same bytes as raw deflate (zlib, wbits=-15)
    deflate_corpus.meta.json  sizes and the plaintext's SHA-256

See deflate_corpus() for why it is shaped the way it is.
"""

import hashlib
import io
import json
import os
import sys

import fastavro
import fastavro.read
from fastavro.schema import fingerprint, to_parsing_canonical_form

HERE = os.path.dirname(os.path.abspath(__file__))


class raw_logicals:
    """Read logical types as their underlying int/long/bytes.

    fastavro turns a `date` into datetime.date and a `decimal` into
    decimal.Decimal on the way out. The Lua implementation deliberately does
    not: it keeps logicalType as schema metadata and hands back the underlying
    value. Disabling the logical readers is what makes the two comparable --
    and it is only the readers, so the fixtures themselves are unaffected.
    """

    def __enter__(self):
        self.saved = dict(fastavro.read.LOGICAL_READERS)
        fastavro.read.LOGICAL_READERS.clear()
        return self

    def __exit__(self, *exc):
        fastavro.read.LOGICAL_READERS.update(self.saved)
        return False


def b(hexstr):
    """A byte string, written as hex for readability."""
    return bytes.fromhex(hexstr)


# Every case is (name, schema, [values]).
CASES = [
    (
        "primitives",
        {
            "type": "record",
            "name": "Primitives",
            "namespace": "pregel.test",
            "fields": [
                {"name": "f_null", "type": "null"},
                {"name": "f_boolean", "type": "boolean"},
                {"name": "f_int", "type": "int"},
                {"name": "f_long", "type": "long"},
                {"name": "f_float", "type": "float"},
                {"name": "f_double", "type": "double"},
                {"name": "f_bytes", "type": "bytes"},
                {"name": "f_string", "type": "string"},
            ],
        },
        [
            {
                "f_null": None,
                "f_boolean": True,
                "f_int": 0,
                "f_long": 0,
                "f_float": 0.0,
                "f_double": 0.0,
                "f_bytes": b(""),
                "f_string": "",
            },
            {
                "f_null": None,
                "f_boolean": False,
                "f_int": -1,
                "f_long": 1,
                "f_float": 1.5,
                "f_double": -2.25,
                "f_bytes": b("00ff10"),
                "f_string": "foo",
            },
            {
                "f_null": None,
                "f_boolean": True,
                "f_int": 2147483647,
                "f_long": 9223372036854775807,
                "f_float": -3.5,
                "f_double": 1e300,
                "f_bytes": b("deadbeef"),
                # Multi-byte UTF-8, so the length prefix is not the character
                # count.
                "f_string": "привет 世界",
            },
            {
                "f_null": None,
                "f_boolean": False,
                "f_int": -2147483648,
                "f_long": -9223372036854775808,
                "f_float": 0.1,
                "f_double": 0.1,
                "f_bytes": b("7f80"),
                "f_string": "a" * 200,
            },
        ],
    ),
    (
        "nested",
        {
            "type": "record",
            "name": "Outer",
            "namespace": "pregel.test",
            "fields": [
                {"name": "id", "type": "long"},
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                            {"name": "label", "type": "string"},
                            {"name": "weight", "type": "double"},
                        ],
                    },
                },
                {"name": "again", "type": "Inner"},
            ],
        },
        [
            {
                "id": 1,
                "inner": {"label": "a", "weight": 0.5},
                "again": {"label": "b", "weight": -0.5},
            },
            {
                "id": -1,
                "inner": {"label": "", "weight": 0.0},
                "again": {"label": "z" * 100, "weight": 1e100},
            },
        ],
    ),
    (
        "enum_fixed",
        {
            "type": "record",
            "name": "EnumFixed",
            "namespace": "pregel.test",
            "fields": [
                {
                    "name": "suit",
                    "type": {
                        "type": "enum",
                        "name": "Suit",
                        "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"],
                    },
                },
                {
                    "name": "digest",
                    "type": {"type": "fixed", "name": "MD5", "size": 16},
                },
            ],
        },
        [
            {"suit": "SPADES", "digest": b("00" * 16)},
            {"suit": "CLUBS", "digest": b("000102030405060708090a0b0c0d0e0f")},
            {"suit": "HEARTS", "digest": b("ff" * 16)},
        ],
    ),
    (
        "array_map",
        {
            "type": "record",
            "name": "Collections",
            "namespace": "pregel.test",
            "fields": [
                {"name": "ints", "type": {"type": "array", "items": "int"}},
                {"name": "strings", "type": {"type": "array", "items": "string"}},
                {"name": "counts", "type": {"type": "map", "values": "long"}},
                {
                    "name": "nested",
                    "type": {
                        "type": "map",
                        "values": {"type": "array", "items": "string"},
                    },
                },
            ],
        },
        [
            {"ints": [], "strings": [], "counts": {}, "nested": {}},
            {
                "ints": [1, -2, 3],
                "strings": ["a", "bb"],
                "counts": {"one": 1},
                "nested": {"k": ["x", "y"]},
            },
            {
                "ints": list(range(200)),
                "strings": ["s%d" % i for i in range(50)],
                "counts": {"k%d" % i: i for i in range(30)},
                "nested": {"a": [], "b": ["z"]},
            },
        ],
    ),
    (
        "union",
        {
            "type": "record",
            "name": "Unions",
            "namespace": "pregel.test",
            "fields": [
                {"name": "maybe_int", "type": ["null", "int"]},
                {"name": "maybe_string", "type": ["null", "string"]},
                {"name": "int_or_string", "type": ["int", "string"]},
                {
                    "name": "maybe_record",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "Payload",
                            "fields": [{"name": "v", "type": "long"}],
                        },
                    ],
                },
            ],
        },
        [
            {
                "maybe_int": None,
                "maybe_string": None,
                "int_or_string": 1,
                "maybe_record": None,
            },
            {
                "maybe_int": 42,
                "maybe_string": "hi",
                "int_or_string": "text",
                "maybe_record": {"v": 7},
            },
            {
                "maybe_int": -1,
                "maybe_string": "",
                "int_or_string": 0,
                "maybe_record": {"v": -9007199254740993},
            },
        ],
    ),
    (
        "logical",
        {
            "type": "record",
            "name": "Logical",
            "namespace": "pregel.test",
            "fields": [
                # The logical types are carried as annotations only: the values
                # below are the underlying int/long/bytes, which is what the Lua
                # side encodes and decodes.
                {"name": "d", "type": {"type": "int", "logicalType": "date"}},
                {
                    "name": "ts",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {
                    "name": "dec",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 9,
                        "scale": 2,
                    },
                },
            ],
        },
        [
            {"d": 0, "ts": 0, "dec": b("00")},
            {"d": 19000, "ts": 1700000000000, "dec": b("04d2")},
            {"d": -1, "ts": -1, "dec": b("fb2e")},
        ],
    ),
    (
        "defaults",
        {
            "type": "record",
            "name": "Defaults",
            "namespace": "pregel.test",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string", "default": "fallback"},
                {"name": "c", "type": ["null", "long"], "default": None},
                {
                    "name": "d",
                    "type": {"type": "array", "items": "int"},
                    "default": [1, 2],
                },
            ],
        },
        [
            {"a": 1, "b": "given", "c": 5, "d": [9]},
            {"a": 2, "b": "fallback", "c": None, "d": [1, 2]},
        ],
    ),
]


def to_json_safe(value):
    """Make a decoded value JSON-representable without losing bytes."""
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    return value


def write(path, data):
    mode = "wb" if isinstance(data, bytes) else "w"
    with open(path, mode) as fh:
        fh.write(data)
    print("  %-28s %7d bytes" % (os.path.basename(path), len(data)))


def deflate_corpus():
    """A plaintext that makes a compressor emit every RFC 1951 length code.

    The pure-Lua inflater in pregel/avro/deflate.lua is what reads a deflate
    block on a build without compress.zlib, and its LENGTH_BASE / LENGTH_EXTRA
    tables are only exercised by the length codes a stream actually uses. The
    Avro fixtures above are a few hundred bytes each and never reach the higher
    codes, so a wrong entry in those tables changed nothing anywhere in the
    suite.

    Matches of every length from 3 to 258 cover all 29 length codes, so the
    corpus is, for each length L, a pseudo-random block of L bytes followed
    immediately by a copy of itself -- which is exactly a back-reference of
    length L. A tail of unrepeated bytes keeps the literal path in the picture
    and stops the whole thing being one long run.

    The PRNG is a plain LCG written out here rather than `random`, so the bytes
    do not depend on the Python version.
    """
    state = 0x2545F491
    def nxt():
        nonlocal state
        state = (state * 1103515245 + 12345) & 0x7FFFFFFF
        return (state >> 16) & 0xFF

    out = bytearray()
    for length in range(3, 259):
        # A small alphabet: enough variety to be worth compressing, small
        # enough that the matcher finds the repeat rather than a longer one.
        block = bytes((nxt() % 24) + 0x61 for _ in range(length))
        out += block
        out += block
    out += bytes(nxt() for _ in range(4096))
    return bytes(out)


def write_deflate_fixtures():
    """Raw deflate streams (wbits=-15, the framing Avro's deflate codec uses)."""
    import zlib

    print("deflate_corpus")
    plain = deflate_corpus()
    write(os.path.join(HERE, "deflate_corpus.raw"), plain)
    levels = {}
    for level in (1, 6, 9):
        c = zlib.compressobj(level, zlib.DEFLATED, -15)
        packed = c.compress(plain) + c.flush()
        assert zlib.decompress(packed, -15) == plain
        name = "deflate_corpus.l%d.z" % level
        write(os.path.join(HERE, name), packed)
        levels[str(level)] = {"file": name, "packed": len(packed)}
    write(os.path.join(HERE, "deflate_corpus.meta.json"),
          json.dumps({
              "plain": "deflate_corpus.raw",
              "plain_bytes": len(plain),
              "sha256": hashlib.sha256(plain).hexdigest(),
              "levels": levels,
              "wbits": -15,
          }, indent=2, sort_keys=True) + "\n")


def main():
    print("fastavro", fastavro.__version__)
    index = []
    for name, schema, records in CASES:
        print(name)
        parsed = fastavro.parse_schema(schema)

        write(os.path.join(HERE, name + ".avsc"),
              json.dumps(schema, indent=2, sort_keys=False) + "\n")

        # The plain binary encoding: every record back to back, no framing.
        raw = io.BytesIO()
        for record in records:
            fastavro.schemaless_writer(raw, parsed, record)
        encoded = raw.getvalue()
        write(os.path.join(HERE, name + ".bin"), encoded)

        # The expectation is what fastavro reads back out of those bytes, not
        # what went in: a float field returns its single-precision rounding, and
        # the point of the file is to say what a correct decoder produces.
        back = io.BytesIO(encoded)
        with raw_logicals():
            decoded = [fastavro.schemaless_reader(back, parsed)
                       for _ in range(len(records))]
        assert back.read() == b"", "trailing bytes in %s.bin" % name
        write(os.path.join(HERE, name + ".json"),
              "\n".join(json.dumps(to_json_safe(r), sort_keys=True)
                        for r in decoded) + "\n")

        canonical = to_parsing_canonical_form(schema)
        write(os.path.join(HERE, name + ".meta.json"),
              json.dumps({
                  "canonical": canonical,
                  "fingerprint": fingerprint(canonical, "CRC-64-AVRO"),
                  "records": len(records),
                  "fastavro": fastavro.__version__,
              }, indent=2, sort_keys=True) + "\n")

        for codec in ("null", "deflate"):
            out = io.BytesIO()
            # A fixed sync marker keeps the fixtures byte-stable across runs.
            fastavro.writer(out, parsed, records, codec=codec,
                            sync_marker=b"pregel-avro-fixt")
            write(os.path.join(HERE, "%s.%s.avro" % (name, codec)),
                  out.getvalue())

        index.append(name)

    write_deflate_fixtures()

    write(os.path.join(HERE, "index.json"),
          json.dumps({"cases": index, "fastavro": fastavro.__version__},
                     indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
