using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme.Tests;

public class AcmeAccountStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, true);
        }
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void SaveLoad_RoundTrips_AndCreatesDirectory(AcmeKeyAlgorithm algorithm)
    {
        AcmeAccountStore store = new(Path.Combine(_dir, "nested", "account.json"), AcmeStoreProtection.None);
        Assert.False(store.Exists);
        Assert.Null(store.Load());
        Assert.Null(store.LoadKey());

        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        Uri account = new("https://ca.test/acct/42");
        AcmeAccountState state = AcmeAccountStore.CreateState(key, account, AcmeDirectories.LetsEncryptStaging);
        store.Save(state);
        Assert.True(store.Exists);
        Assert.False(File.Exists(store.Path + ".tmp"));

        AcmeAccountState loaded = store.Load()!;
        Assert.Equal(account, loaded.AccountUrl);
        Assert.Equal(AcmeDirectories.LetsEncryptStaging, loaded.DirectoryUrl);
        Assert.Equal(algorithm, loaded.Algorithm);
        Assert.Equal(state.PrivateKeyPem, loaded.PrivateKeyPem);

        using AcmeAccountKey loadedKey = store.LoadKey()!;
        Assert.Equal(key.Thumbprint, loadedKey.Thumbprint);

        string json = File.ReadAllText(store.Path);
        Assert.Contains("\"algorithm\":\"" + algorithm + "\"", json);

        // Overwrite works (temp + rename).
        store.Save(state with { AccountUrl = new Uri("https://ca.test/acct/43") });
        Assert.Equal("https://ca.test/acct/43", store.Load()!.AccountUrl.AbsoluteUri);

        store.Delete();
        Assert.False(store.Exists);
        store.Delete(); // idempotent
    }

    [Fact]
    public void Load_RejectsCorruptFiles()
    {
        Directory.CreateDirectory(_dir);
        string path = Path.Combine(_dir, "account.json");
        File.WriteAllText(path, "{ not json");
        AcmeAccountStore store = new(path);
        Assert.Throws<InvalidDataException>(() => store.Load());

        File.WriteAllText(path, "null");
        Assert.Throws<InvalidDataException>(() => store.Load());
    }

    [Fact]
    public void PendingOrder_RoundTrips_AndRequiresState()
    {
        AcmeAccountStore store = new(Path.Combine(_dir, "account.json"));
        AcmePendingOrder pending = new()
        {
            OrderUrl = new Uri("https://ca.test/order/1"),
            Identifiers = [AcmeIdentifier.Dns("a.test"), AcmeIdentifier.Ip("192.0.2.1")],
            CertificateKeyPem = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
            Expires = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero),
            CreatedAt = new DateTimeOffset(2026, 9, 24, 12, 0, 0, TimeSpan.Zero),
        };
        Assert.Throws<InvalidOperationException>(() => store.SavePendingOrder(pending));

        using AcmeAccountKey key = AcmeAccountKey.Create();
        store.Save(AcmeAccountStore.CreateState(key, new Uri("https://ca.test/acct/1"), AcmeDirectories.LetsEncrypt));
        Assert.Null(store.Load()!.PendingOrder);
        Assert.DoesNotContain("pendingOrder", File.ReadAllText(store.Path));

        store.SavePendingOrder(pending);
        AcmePendingOrder loaded = store.Load()!.PendingOrder!;
        Assert.Equal(pending, loaded with { Identifiers = pending.Identifiers });
        Assert.Equal(pending.Identifiers, loaded.Identifiers);
        Assert.Equal(key.Thumbprint, store.LoadKey()!.Thumbprint); // the account key is untouched

        store.SavePendingOrder(null);
        Assert.Null(store.Load()!.PendingOrder);
    }

    [Theory]
    [InlineData(AcmeStoreProtection.DpapiCurrentUser)]
    [InlineData(AcmeStoreProtection.DpapiLocalMachine)]
    public void Dpapi_ProtectsTheDocument_OnWindows(AcmeStoreProtection protection)
    {
        string path = Path.Combine(_dir, "protected.json");
        if (!OperatingSystem.IsWindows())
        {
            Assert.Throws<PlatformNotSupportedException>(() => new AcmeAccountStore(path, protection));
            return;
        }

        AcmeAccountStore store = new(path, protection);
        Assert.Equal(protection, store.Protection);
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeAccountState state = AcmeAccountStore.CreateState(key, new Uri("https://ca.test/acct/7"), AcmeDirectories.ZeroSsl);
        store.Save(state);

        string raw = File.ReadAllText(path);
        Assert.Contains("\"dpapi\":", raw);
        Assert.DoesNotContain("PRIVATE KEY", raw);
        Assert.DoesNotContain("acct/7", raw);

        Assert.Equal(state, store.Load());
        Assert.Equal(key.Thumbprint, store.LoadKey()!.Thumbprint);

        // A plain store reads DPAPI files (and vice versa): protection is a write-time choice.
        AcmeAccountStore plain = new(path, AcmeStoreProtection.None);
        Assert.Equal(AcmeStoreProtection.None, plain.Protection);
        Assert.Equal(state, plain.Load());
        plain.SavePendingOrder(new AcmePendingOrder { OrderUrl = new Uri("https://ca.test/order/9"), Identifiers = [AcmeIdentifier.Dns("x.test")], CertificateKeyPem = "k" });
        Assert.DoesNotContain("dpapi", File.ReadAllText(path));
        Assert.Equal("https://ca.test/order/9", store.Load()!.PendingOrder!.OrderUrl.AbsoluteUri);
        store.SavePendingOrder(null); // re-protects
        Assert.Contains("\"dpapi\":", File.ReadAllText(path));

        // Corrupt blobs are reported as InvalidDataException.
        File.WriteAllText(path, "{\"dpapi\":\"AAAA\"}");
        Assert.Throws<InvalidDataException>(() => store.Load());
        File.WriteAllText(path, "{\"dpapi\":\"not base64!\"}");
        Assert.Throws<InvalidDataException>(() => store.Load());
        File.WriteAllText(path, "{\"dpapi\":null}");
        Assert.Throws<InvalidDataException>(() => store.Load()); // decodes as an empty (invalid) state document
    }

    [Fact]
    public void Path_IsMadeAbsolute()
    {
        AcmeAccountStore store = new("relative-account.json");
        Assert.True(Path.IsPathRooted(store.Path));
        Assert.Equal(Path.GetFullPath("relative-account.json"), store.Path);
    }

    [Fact]
    public void ArgumentValidation()
    {
        Assert.Throws<ArgumentException>(() => new AcmeAccountStore(""));
        AcmeAccountStore store = new(Path.Combine(_dir, "x.json"));
        Assert.Throws<ArgumentNullException>(() => store.Save(null!));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Assert.Throws<ArgumentNullException>(() => AcmeAccountStore.CreateState(null!, new Uri("https://a/"), new Uri("https://b/")));
        Assert.Throws<ArgumentNullException>(() => AcmeAccountStore.CreateState(key, null!, new Uri("https://b/")));
        Assert.Throws<ArgumentNullException>(() => AcmeAccountStore.CreateState(key, new Uri("https://a/"), null!));
    }
}
