using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Testing.Tests.Acme;

/// <summary>
/// Every <see cref="TestCa"/> is a fresh root, so no two may look like the same issuer, and a root that SslStream copied
/// into the Windows intermediate store must not outlive its CA: such copies used to pile up across test runs under one
/// name until Windows chain building failed for every certificate that name issued.
/// </summary>
public class TestCaTests
{
    /// <summary>Windows chain building fails once more than 50 same-named certificates are candidates for one issuer.</summary>
    private const int ManyEarlierRoots = 60;

    [Fact]
    public void Two_cas_created_with_one_name_have_distinct_subjects()
    {
        using TestCa first = new("Fake Root CA");
        using TestCa second = new("Fake Root CA");

        Assert.NotEqual(first.Root.Subject, second.Root.Subject);
        Assert.StartsWith("CN=Fake Root CA", first.Root.Subject, StringComparison.Ordinal);
        Assert.Equal("CN=" + first.Name, first.Root.Subject);
    }

    [Fact]
    public void A_chain_still_builds_while_many_earlier_roots_of_the_same_name_are_around()
    {
        // What earlier test runs left behind: roots created with the same name, none of which signed this leaf.
        List<TestCa> earlier = [];
        X509Certificate2Collection leftovers = [];
        try
        {
            for (int i = 0; i < ManyEarlierRoots; i++)
            {
                TestCa ca = new("Fake Root CA");
                earlier.Add(ca);
                leftovers.Add(X509CertificateLoader.LoadCertificate(ca.Root.RawData));
            }

            using TestCa current = new("Fake Root CA");
            DateTimeOffset now = DateTimeOffset.UtcNow;
            using X509Certificate2 leaf = current.IssueLeaf("game.example.test", now.AddMinutes(-5), now.AddDays(1));

            // The issuer is nowhere to be found: a partial chain, not "An unknown chain building error occurred".
            using (X509Chain chain = NewChain(leftovers))
            {
                Assert.False(chain.Build(leaf));
                Assert.Contains(chain.ChainStatus, s => s.Status.HasFlag(X509ChainStatusFlags.PartialChain));
                DisposeElements(chain);
            }

            // With the issuer at hand the chain reaches it (untrusted, as a test root is).
            using X509Certificate2 root = X509CertificateLoader.LoadCertificate(current.Root.RawData);
            using (X509Chain chain = NewChain([.. leftovers, root]))
            {
                chain.Build(leaf);
                Assert.Equal(2, chain.ChainElements.Count);
                Assert.Equal(current.Root.Thumbprint, chain.ChainElements[1].Certificate.Thumbprint);
                DisposeElements(chain);
            }
        }
        finally
        {
            foreach (X509Certificate2 certificate in leftovers)
            {
                certificate.Dispose();
            }

            foreach (TestCa ca in earlier)
            {
                ca.Dispose();
            }
        }
    }

    [Fact]
    public void Dispose_removes_the_copy_of_the_root_that_SslStream_put_in_the_intermediate_store()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("Only Windows copies a chain's CA certificates into a certificate store");
        }

        TestCa ca = new("Fake Root CA");
        string thumbprint = ca.Root.Thumbprint;
        try
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            using X509Certificate2 leaf = ca.IssueLeaf("game.example.test", now.AddMinutes(-5), now.AddDays(1));
            using X509Certificate2 root = X509CertificateLoader.LoadCertificate(ca.Root.RawData);

            // What a TLS endpoint does with an issued certificate and its chain. The OS cannot build that chain alone, so the
            // runtime copies the root into the "Intermediate Certification Authorities" store (LocalMachine when elevated).
            SslStreamCertificateContext.Create(leaf, [root], offline: true);
            Assert.True(InIntermediateStore(thumbprint), "SslStream no longer copies the chain into a store; this test and TestCa.Dispose's cleanup can go.");
        }
        finally
        {
            ca.Dispose();
        }

        Assert.False(InIntermediateStore(thumbprint));
    }

    private static X509Chain NewChain(X509Certificate2Collection extra)
    {
        X509Chain chain = new();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.DisableCertificateDownloads = true;
        chain.ChainPolicy.ExtraStore.AddRange(extra);
        return chain;
    }

    private static void DisposeElements(X509Chain chain)
    {
        foreach (X509ChainElement element in chain.ChainElements)
        {
            element.Certificate.Dispose();
        }
    }

    private static bool InIntermediateStore(string thumbprint)
    {
        foreach (StoreLocation location in (StoreLocation[])[StoreLocation.CurrentUser, StoreLocation.LocalMachine])
        {
            using X509Store store = new(StoreName.CertificateAuthority, location);
            store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
            X509Certificate2Collection all = store.Certificates;
            bool present = false;
            foreach (X509Certificate2 certificate in all)
            {
                present |= string.Equals(certificate.Thumbprint, thumbprint, StringComparison.OrdinalIgnoreCase);
                certificate.Dispose();
            }

            if (present)
            {
                return true;
            }
        }

        return false;
    }
}
