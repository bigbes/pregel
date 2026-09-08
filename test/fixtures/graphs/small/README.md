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

`weighted9.txt` and `weighted9/` are the graph `examples/sssp` measures
distances over, in the same two forms and produced the same way. Nine vertices
named `a` … `i`, twelve weighted edges:

    a -1-> b      b -2-> c      c -1-> d      d -2-> f      f -1-> g
    a -4-> c      b -5-> d      c -3-> e      e -1-> d      g -2-> h
                                              e -7-> f      i -1-> a

The weights are chosen so the first route found is not the shortest one: from
`a`, `c` is 3 through `b` rather than the direct 4, and `d` is 4 through `c`
rather than the 6 it hears from `b` a superstep earlier. `i` has an out-edge
and no in-edge, so it stays unreachable whichever of the other eight is the
source. The vertex `value` column is 0 throughout — sssp takes its starting
distances from the source name, not from the file.
