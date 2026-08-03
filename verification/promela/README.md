# Flowie and FlowMQ Promela Models

`flowie_flowmq_lifecycle.pml` is a finite-state model for lifecycle ordering.
It does not translate the C implementation and does not model TCP buffering,
MQTT byte parsing, CoroNet internals, or throughput.

The Flowie portion models the raw TCP framing contract exercised by
`flowie/tests/test_flowie_transport.c`: a PINGRESP is non-terminal, so the
client may still send DISCONNECT. The FlowMQ portion models accepted async
sends, which must receive exactly one terminal completion even when shutdown
races with owner-lane processing.

Source mappings:

- Flowie PINGRESP enqueue without terminal close: `flowie/src/flowie_endpoint.c`.
- Flowie raw framing client sends DISCONNECT after PINGRESP:
  `flowie/tests/test_flowie_transport.c`.
- FlowMQ async worker drains accepted requests through batch submission and
  completes every request: `flowmq/src/flow_fmq.c`.

Run from a temporary directory so SPIN-generated `pan.*` files stay outside
the repository. On this Windows host, use Clang as both the SPIN preprocessor
and verifier compiler:

```powershell
Copy-Item verification/promela/flowie_flowmq_lifecycle.pml C:/tmp/
Set-Location C:/tmp
$clang = 'C:/Users/lockg/scoop/apps/llvm/current/bin/clang.exe'
& 'C:/tools/cpp-dev/bin/spin.exe' "-P$clang -E -x c" -a flowie_flowmq_lifecycle.pml
& $clang -O2 -DWIN32 -Dopen=_open -Dwrite=_write -DNFAIR=3 -include io.h -o pan-clang.exe pan.c

# SPIN verifies one claim per run. -f enables weak fairness for liveness.
./pan-clang.exe -a -N no_early_flowie_eof
./pan-clang.exe -a -N no_duplicate_flowmq_completion
./pan-clang.exe -a -f -N flowie_disconnect_is_delivered
./pan-clang.exe -a -f -N accepted_flowmq_send_completes_once
```

The Windows-only `open`/`write` mappings and `io.h` inclusion adapt the
POSIX names emitted by this SPIN version; they do not change model semantics.

Every counterexample must become a focused C regression test before changing
the production lifecycle behavior.
