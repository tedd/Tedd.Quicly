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

## Server integration (`Tedd.Quicly.Server.Certificates`)

`CertificateProvisioner` is the glue a game server uses. It reuses the pieces above: `AcmeCertificateManager` for the
order flow, `RenewalScheduler` for renewals, and `HttpServer` with `Http01ChallengeHandler`, `RedirectToHttpsHandler`,
`HealthHandler` and `TlsAlpn01Responder` for the challenge endpoints. It implements `ICertificateSource`, so it serves the
same way whether the certificate is fixed, comes from a file, or comes from ACME.

### Enabling ACME

```csharp
var certificates = new CertificateProvisioner(ServerCertificateOptions.Acme(new AcmeProvisioningOptions
{
    DirectoryUrl = AcmeDirectories.LetsEncryptStaging,   // switch to AcmeDirectories.LetsEncrypt once it works
    AgreeToTermsOfService = true,
    Contacts = { "mailto:ops@example.com" },
    DnsNames = { "play.example.com" },
    AccountStorePath = "/var/lib/mygame/acme-account.json",
    CertificatePath = "/var/lib/mygame/server.pfx",
}));
certificates.StatusChanged += s => logger.Log(s.ToString());   // Starting, Valid, Renewing, Failed(reason), Stopped
await certificates.StartAsync(ct);
X509Certificate2 first = await certificates.WaitForCertificateAsync(ct);
```

`StartAsync` throws only for problems that retrying cannot fix: invalid options, an endpoint that cannot be bound,
or an unreadable PFX file. A CA that cannot issue a certificate does not make it throw. The status becomes `Failed`
with the reason, and the provisioner keeps retrying in the background (the manager's back-off within an attempt, then
`Renewal.RetryDelay` between attempts). `WaitForCertificateAsync` completes once a certificate exists. Background work
never throws: every failure is reported through `Status`/`StatusChanged`. Non-fatal problems (for example an event
handler that threw) go to `Error`, and every ACME stage goes to `Progress`, for logging.

On start, a persisted certificate at `CertificatePath` is served without contacting the CA if it covers every
configured name, has not expired, and is not due for renewal. A restart therefore costs no rate limit. A persisted
certificate that is valid but due is served while a replacement is ordered. After that, `RenewalScheduler` renews at
the CA's ARI window, or when one third of the lifetime remains (`Renewal.RenewBefore` overrides this). `RenewNowAsync`
forces a renewal, for example on key compromise or after the CA revoked the certificate. It cuts a pending retry wait
short, and calls made while such an order is queued or running share its outcome. (Changed names need a restart: the
persisted certificate then no longer covers them and a new one is ordered.)

### Ports

| Challenge (`ChallengeTypes`) | Needs | Endpoint option |
| --- | --- | --- |
| `Http01` (default, first) | TCP 80, reachable from the internet at every name's address | `HttpChallengeEndpoint` (default `0.0.0.0:80`) |
| `TlsAlpn01` (default, second) | TCP 443 | `TlsAlpnEndpoint` (default `0.0.0.0:443`) |
| `Dns01` | no inbound port; an `IDns01Provider` for your DNS host; the only way to get wildcards | `Dns01Provider`, `ChallengePropagationDelay` |

QUIC uses UDP, so the game's QUIC listener on UDP 443 and the `tls-alpn-01` endpoint on TCP 443 do not conflict. Only
the listed challenge types are served. List just `Dns01` for a server that must not open TCP ports. The HTTP endpoint
answers the challenge path and, by default, redirects everything else to HTTPS (`RedirectToHttps`, ADR 0009).
`EnableHealthEndpoint` adds `/healthz` there. Normal TLS clients on the TLS endpoint get the current certificate; add
handlers with `TlsEndpointHandlers`. For IP identifiers (RFC 8738), the challenge certificate is also published under the
reverse-DNS name, which is the SNI the CA sends.

### Hot swap into running listeners

`CertificateBinder` applies the source's current certificate to any number of `ICertificateConsumer`s. It does so
immediately, and again on every `Changed`:

```csharp
var httpsSource = new StaticCertificateSource();                 // an HTTPS endpoint's HttpTlsOptions.CertificateSource
await using var binder = new CertificateBinder(certificates, [
    CertificateConsumers.FromDelegate(quicListener.UpdateCertificate),   // e.g. a new MsQuic configuration for new connections
    CertificateConsumers.FromSource(httpsSource),
]);
binder.ConsumerFailed += f => logger.Log(f.Exception);
```

Each consumer switches only new handshakes to the new certificate; established connections are not affected. A
consumer that throws is reported and does not stop the others. The replaced certificate is disposed only after
`SupersededCertificateGracePeriod` (default 2 minutes), because handshakes that had already selected it still need its
private key to sign. On Windows, disposing a PKCS#12-imported certificate deletes its key container. This matches
ADR 0009: open the new configuration, swap it in, and close the old one when nothing uses it. Stop the listeners before
disposing the binder, and dispose the binder before the provisioner.

### Staging, production and EAB

Test a deployment against a staging directory (`AcmeDirectories.LetsEncryptStaging`, `BuypassTest`,
`GoogleTrustServicesTest`). Staging has much higher rate limits but issues untrusted certificates. Then switch
`DirectoryUrl` to production. The account store records its directory, so switching creates a new account
automatically. Delete `CertificatePath` when you switch, so that the staging certificate is not served until it is
due.

ZeroSSL, Google Trust Services and SSL.com require External Account Binding (`AcmeDirectories.KnownCas` lists which
CAs do). Copy the key id and HMAC key from the CA's dashboard, or from `gcloud publicca external-account-keys create`
for Google:

```csharp
DirectoryUrl = AcmeDirectories.ZeroSsl,
ExternalAccountKeyId = "kid-from-dashboard",
ExternalAccountHmacKey = secrets["zerossl-eab-hmac"],   // only signs account creation; never stored or logged
```

SSL.com's RSA directory needs `KeyAlgorithm = AcmeKeyAlgorithm.RS256`. A private CA (Pebble, in-house) needs
`HttpClient = new HttpClient(AcmeClient.CreateHttpHandler(roots))` so that its root is trusted.

### Static and file certificates

`ServerCertificateOptions.Static(cert)` serves a certificate the application owns. The provisioner never disposes it.
`ServerCertificateOptions.File(path, password, reloadOnChange: true)` serves a PFX and polls its modification time
(`ReloadInterval`, default 10 s). Replace the file atomically (write a temporary file, then rename it). A replaced file
raises `Changed`, and the binder swaps it in like a renewal. A file that cannot be read reports `Failed` and keeps the
previous certificate.

### Testing

`Tedd.Quicly.Testing.Acme.FakeAcmeServer` is an in-process ACME CA for tests (never reference it from production
code). Besides callback validation, it validates like a real CA:

* `Http01ValidationHost`/`Port`: `GET /.well-known/acme-challenge/{token}` with the identifier as `Host`, with no
  redirects followed.
* `TlsAlpnValidationHost`/`Port`: a TLS handshake with SNI and ALPN `acme-tls/1`, checking the single SAN and the
  critical `acmeIdentifier` hash.
* `DnsTxtLookup`: an injected TXT table such as `InMemoryDns01Provider.Lookup`.

`CertificateLifetime`, `UnavailableRequestsRemaining` and `FailValidation` drive the renewal, outage and failure paths.
Bind every endpoint to `127.0.0.1:0` and pass the bound ports (`CertificateProvisioner.HttpChallengeEndPoint` /
`TlsEndPoint`) to the fake.
