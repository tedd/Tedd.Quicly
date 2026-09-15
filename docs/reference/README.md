# Reference material

- `msquic/msquic.h`, `msquic/msquic_winuser.h` — MsQuic 2.6.1 public headers (MIT, see `msquic/LICENSE`), taken from the `Microsoft.Native.Quic.MsQuic.OpenSSL` NuGet package. Used as the source of truth for constants (`QUIC_PARAM_*`, `QUIC_SETTINGS` bitfields).
- `msquic/msquic_layout_dotnet11_preview7.txt` — exact struct sizes / field offsets / enum values of the MsQuic interop types, dumped by reflection from the `Microsoft.Quic` namespace inside `System.Net.Quic.dll` (.NET 11 preview 7, which bundles msquic 2.5.9). Our hand-written bindings are validated against these numbers by unit tests.
- `h3_tables.txt` — the HPACK/QPACK Huffman code table (RFC 7541 Appendix B; codes left-aligned in 32 bits + bit lengths) and the QPACK static table (RFC 9204 Appendix A), dumped from the shared-source copies inside Kestrel. Used to generate the tables in `Tedd.Quicly.Http3`.
