# ACME certificates (`Tedd.Quicly.Acme`)

`Tedd.Quicly.Acme` is an RFC 8555 ACME v2 client for any ACME CA (Let's Encrypt, ZeroSSL, Buypass, Google Trust
Services, SSL.com, Pebble, in-house CAs). It uses only in-box APIs: `System.Net.Http`, `System.Text.Json` with a
source-generated context, and `System.Security.Cryptography`. ADR 0009 sets the policy; this document is the consumer
contract, for the Server/Http glue and for applications that drive the client directly.

## Layers

| Type | Role |
| --- | --- |
| `AcmeDirectories` | Known directory URLs and which CAs require External Account Binding (EAB). |
| `AcmeAccountKey`, `JwsSigner` | ES256 (default) / RS256 keys, JWK + RFC 7638 thumbprint, flattened JWS, EAB (HS256). |
| `AcmeClient` | One method per RFC 8555 resource: account, order, authorization, challenge, finalize, download, revoke, ARI. |
| `CsrBuilder`, `IssuedCertificate` | PKCS#10 CSR (SAN DNS + IP) and the chain + key combination (PKCS#12 round trip). |
| `AcmeCertificateManager` | The whole order flow, with always-cleanup, crash resume, and retry with back-off. |
| `RenewalScheduler` | When to renew (ARI, else one third of the lifetime) and a cancellation-aware renewal loop. |

## Transport security

* Only `https` URLs are requested (RFC 8555 §6.1). This covers the directory URL, every directory entry, and each
  `Location`, `Link`, order, authorization, challenge, certificate and `renewalInfo` URL the CA hands out. Plain
  `http` is accepted only for loopback hosts, or when `AcmeClientOptions.AllowInsecureHttp` is set for a mock CA
  elsewhere. A refused URL raises `AcmeException` with type `AcmeErrorTypes.InsecureUrl` before anything is sent. A
  non-https directory URL is rejected by the constructor with `ArgumentException`.
* Certificate validation is done by the injected `HttpClient` and is never turned off. To trust a private or mock CA
  root (Pebble prints its own), use `new HttpClient(AcmeClient.CreateHttpHandler(roots))`. That handler uses custom
  root trust, keeps host-name validation, and skips revocation checking (private roots rarely publish CRL / OCSP).
* Responses are size-bounded (`MaxResponseBytes`, 1 MiB by default). The nonce pool is bounded, and invalid
  `Replay-Nonce` values are ignored.

## Secrets

* **Account key.** `AcmeAccountStore` persists the key, the account URL (`kid`), the directory URL and any in-flight
  order as JSON. Writes are atomic (temp file + rename). The default protection is DPAPI for the current user on
  Windows (`AcmeStoreProtection.DpapiCurrentUser`) and an owner-only `0600` file elsewhere. Use `DpapiLocalMachine`
  for services that change accounts, or `None` when the file must be portable. Any store instance reads either
  format.
* **Never logged.** `AcmeAccountState`, `AcmePendingOrder` and `ExternalAccountBinding` redact their secret members
  (`***`) in `ToString()` and in the debugger. `AcmeAccountKey` has no textual form.
* **EAB HMAC key.** It is used only to sign the EAB JWS on `newAccount` and is not persisted by the store.
* **Certificate PFX.** `AcmeCertificateManagerOptions.CertificatePath` holds the certificate's private key. It is
  written atomically with mode `0600` on Unix; set `CertificatePassword` to encrypt it. `IssuedCertificate.Load(pfx,
  password, flags)` lets the TLS glue choose the key storage flags its certificate path needs (ADR 0009).

## Challenge responders

The manager selects one challenge per authorization, in the order given by
`PreferredChallengeTypes`, from those with a configured responder. It publishes the response, asks the CA to
validate it, and waits for the authorization. It then **always** removes the response: on success, on failure and on
cancellation. Cleanup is bounded by `ChallengeCleanupTimeout` (30 s by default, wall clock). A failed or timed-out
cleanup is reported through `Progress` and never replaces the validation outcome.

| Challenge | Interface | Contract |
| --- | --- | --- |
| `http-01` | `IHttp01Responder` | Serve `keyAuthorization` as `application/octet-stream` at `http://<domain>/.well-known/acme-challenge/<token>` on port 80 (`Http01Challenge.GetPath`). Tokens match `[A-Za-z0-9_-]{22,}`; reject anything else. Keep the entry until `RemoveAsync`. A token that is never removed (crash) should expire after about 10 minutes. |
| `dns-01` | `IDns01Provider` | Create a TXT record `_acme-challenge.<domain>` (`Dns01Challenge.GetRecordName`; a wildcard `*.example.com` uses `_acme-challenge.example.com`) with the given value. Several values may coexist. `RemoveTxtAsync` removes only that value. Set `ChallengePropagationDelay` when the provider is eventually consistent. |
| `tls-alpn-01` | `ITlsAlpn01Responder` | For a TLS ClientHello offering ALPN `acme-tls/1` whose SNI names the identifier, present the certificate passed to `PublishAsync`. That certificate is self-signed and carries the critical `id-pe-acmeIdentifier` extension (`1.3.6.1.5.5.7.1.31`) holding SHA-256(keyAuthorization). Complete the handshake and send no application data. The manager disposes its copy after `RemoveAsync`, so copy it if you keep it. For IP identifiers (RFC 8738) the `domain` argument is the IP literal and the certificate's SAN is an `iPAddress`. The CA sends the reverse-DNS name (`in-addr.arpa` / `ip6.arpa`) as SNI, so map it back to the IP. |

## Order flow and failure handling

1. **Account.** The manager creates the account, or reuses the stored one. A reused account is verified once with a
   POST-as-GET to its URL. If the CA answers `accountDoesNotExist` or `unauthorized`, or reports a status other than
   `valid` (a CA reset, a deactivated or revoked account), the manager creates a new account with a new key and saves
   it.
2. **Order.** A pending order persisted by a crashed run is resumed when it is for the same identifiers and still
   usable. Otherwise a new order and certificate key are created and persisted before any challenge is touched. With
   `ReuseKey`, the persisted certificate's key is reused (needed when clients pin the SPKI).
3. **Replacement (ARI).** A renewal carries the replaced certificate's ARI identifier as `replaces` (RFC 9773). If
   the CA rejects that field (`alreadyReplaced`, or `malformed` about `replaces`), the order is sent once more without
   it. Other errors are not retried that way.
4. **Finalize.** The CSR's subject CN is the first DNS identifier of 64 characters or fewer. Long names and IP-only
   orders get an empty subject and a critical SAN. The chain is downloaded as PEM. `PreferredChainIssuer` can select
   an alternate chain (`Link: rel="alternate"`).
5. **Retries.** Transient failures (`rateLimited`, `badNonce`, `serverInternal`, 5xx, 429, transport errors) are
   retried with exponential back-off and jitter, and a server `Retry-After` is a lower bound. A single `badNonce` is
   retried inside the client with the nonce that came back in the error response.

## Renewal policy (`RenewalScheduler`)

* When the CA advertises `renewalInfo`, renewal happens at one uniformly drawn time inside the ARI window. The draw
  is kept while the window is unchanged. ARI is re-fetched at the server's `Retry-After`, clamped to
  [1 minute, 24 hours], or else every `RenewalInfoRefreshInterval`. ARI failures fall back to the lifetime rule.
* Without ARI, renewal happens when one third of the lifetime remains, or at `notAfter - RenewBefore`. A
  `RenewBefore` of at least the whole lifetime (such as short-lived profiles) falls back to the one-third rule.
* A restart never renews by itself unless the certificate expires within `ImmediateRenewalThreshold` (24 h).
  Successful renewals are at least `MinimumRenewalInterval` (1 h) apart.
* Every wait is clamped to 30 days per timer; the loop re-evaluates after each wait.

## Hot swap

`RunAsync(current, onRenewed, onError, ct)` hands every new `IssuedCertificate` to `onRenewed`, and the callee owns
it. The Server glue swaps the listener certificate there. `Certificate` is the leaf with its key, loaded `Exportable`
for this library's own use; `Chain` and `Pfx` are also available. Per ADR 0009 (which amends ADR 0006), give
Schannel / MsQuic `Pfx` as a PKCS#12 credential, or `IssuedCertificate.Load(pfx, password, PersistKeySet |
UserKeySet)` (`MachineKeySet` for services). The glue then disposes the previous certificate once no handshake uses
it. Failures from the order or from the callback go to `onError` and are retried after
`RetryDelay`.
