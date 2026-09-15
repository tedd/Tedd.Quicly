# Benchmarks — Tedd.Quicly.Acme

The ACME client is not on the game hot path (a handful of HTTPS round trips every ~60 days), so correctness and
clarity win over micro-optimisation. The one primitive that is called for every JWS header, payload, signature, JWK
coordinate, thumbprint, DNS TXT value and CSR is base64url encoding/decoding, so that is what was measured
(method per [ADR 0007](../adr/0007-measurement-method.md)).

## Base64url codec: V0 (string surgery) vs V1 (`System.Buffers.Text.Base64Url`)

**V0** (`benchmarks/Tedd.Quicly.Archive/Acme/Base64UrlV0.cs`): `Convert.ToBase64String(...)`, then
`.TrimEnd('=')`, `.Replace('+', '-')`, `.Replace('/', '_')`; decoding re-pads and reverses the replacements before
`Convert.FromBase64String`. This is the textbook implementation found in most ACME clients.

**V1** (`src/Tedd.Quicly.Acme/Base64UrlCodec.cs`): the in-box `System.Buffers.Text.Base64Url` (.NET 9+), which
encodes straight to the URL-safe alphabet without padding and offers span overloads.

### Hypothesis

V0 allocates up to four intermediate strings per encode (the padded base64 string plus one per `TrimEnd`/`Replace`
that changes something) and walks the string up to three extra times. V1 produces the final alphabet in a single
vectorised pass, so it should be **2–4× faster and allocate exactly one string** (or nothing with the span overload).
Decoding should show a similar but smaller gain (V0 always allocates the re-padded string before decoding).

### Measurement

* `dotnet run -c Release --project benchmarks/Tedd.Quicly.Benchmarks --framework net10.0 -- --filter '*Base64UrlBench*' --job short`
* `[MemoryDiagnoser]`, `[ShortRunJob]`; sizes 32 B (thumbprint / TXT value), 256 B (RSA-2048 modulus or signature),
  1 KiB (CSR).
* BenchmarkDotNet 0.15.8 does not yet recognise the `net11.0` runtime moniker, so the run is on .NET 10; the code
  paths are identical on .NET 11.
