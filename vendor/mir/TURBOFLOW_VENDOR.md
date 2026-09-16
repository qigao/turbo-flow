# MIR Vendor Record

- Upstream: https://github.com/vnmakarov/mir
- API version: 0.2, as declared by the imported sources.
- License: MIT; see `LICENSE`.
- Integration: TurboFlow links `mir_static` privately for expression
  interpretation and JIT generation.
- Local modifications: the imported MIR sources are used without TurboFlow
  source patches; build integration is provided by this repository's CMake
  target.

The original upstream commit identifier was not retained with this import.
Future vendor updates must record the exact upstream tag or commit here before
replacing these sources.
