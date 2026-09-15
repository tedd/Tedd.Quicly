# ADR 0006 — The game server is also an HTTP server; certificates come from any ACME CA

**Status:** accepted (2026-09-15)

## Decision
* The QUIC listener serves ALPN `h3` as well: WebTransport sessions and plain HTTP/3 GETs (static files,
  health) are answered by `Tedd.Quicly.Http3` + the MsQuic transport.
* `Tedd.Quicly.Http` provides a small HTTP/1.1 server (TCP; plain and `SslStream`) for ACME `http-01`
  (port 80), `tls-alpn-01` (port 443, ALPN `acme-tls/1`) and general static/health endpoints.
* `Tedd.Quicly.Acme` implements RFC 8555 with a pluggable directory URL and External Account Binding, so
  Let's Encrypt, ZeroSSL, Buypass, Google Trust Services, SSL.com or any other ACME v2 CA works. DNS-01 is
  a provider interface (no DNS vendor SDKs in the library).
* `ICertificateSource` (static file / PFX / ACME) feeds both the QUIC listener and the TLS HTTP endpoint;
  renewal swaps the certificate without restarting the listener.
* Windows/Schannel needs a persisted private key: certificates are imported with a key container
  (`X509KeyStorageFlags.Exportable`, not `EphemeralKeySet`) before being handed to MsQuic.
