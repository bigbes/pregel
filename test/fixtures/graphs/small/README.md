# Small graph fixtures

`ring10.txt` is a hand-written graph in the same two-section text format as
`../soc-Epinions-custom.txt`: ten named vertices (names with spaces, so the
parser is held to the real format), then fourteen edges — the ring
1 → 2 → … → 10 → 1, plus four chords out of vertex 1. Vertex 1 having five
outgoing edges is what lets a test watch `loader.avro_files` split one source's
edges across several `edge.store` batches.

`ring10/vertices.avro` and `ring10/edges.avro` are the same graph in the Avro
form `loader.avro_files` reads, produced from the text file by the converter:

    tarantool tools/text2avro.lua \
        test/fixtures/graphs/small/ring10.txt \
        test/fixtures/graphs/small/ring10 --codec null

Run it from the repository root. The `null` codec keeps the bytes readable in
any Avro implementation and keeps the fixture small enough to be worth
committing. Regenerating produces a byte-different file even from identical
input: the OCF sync marker is random per file.
