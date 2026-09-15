namespace Tedd.Quicly.Acme.Tests;

public class AcmeDirectoriesTests
{
    [Fact]
    public void Constants_MatchPublishedDirectoryUrls()
    {
        Assert.Equal("https://acme-v02.api.letsencrypt.org/directory", AcmeDirectories.LetsEncrypt.AbsoluteUri);
        Assert.Equal("https://acme-staging-v02.api.letsencrypt.org/directory", AcmeDirectories.LetsEncryptStaging.AbsoluteUri);
        Assert.Equal("https://acme.zerossl.com/v2/DV90", AcmeDirectories.ZeroSsl.AbsoluteUri);
        Assert.Equal("https://api.buypass.com/acme/directory", AcmeDirectories.Buypass.AbsoluteUri);
        Assert.Equal("https://api.test4.buypass.no/acme/directory", AcmeDirectories.BuypassTest.AbsoluteUri);
        Assert.Equal("https://dv.acme-v02.api.pki.goog/directory", AcmeDirectories.GoogleTrustServices.AbsoluteUri);
        Assert.Equal("https://dv.acme-v02.test-api.pki.goog/directory", AcmeDirectories.GoogleTrustServicesTest.AbsoluteUri);
        Assert.Equal("https://acme.ssl.com/sslcom-dv-rsa", AcmeDirectories.SslComRsa.AbsoluteUri);
        Assert.Equal("https://acme.ssl.com/sslcom-dv-ecc", AcmeDirectories.SslComEcc.AbsoluteUri);
    }

    [Fact]
    public void KnownCas_ListsEveryConstantWithEabRequirement()
    {
        Assert.Equal(9, AcmeDirectories.KnownCas.Count);
        Assert.False(AcmeDirectories.Find(AcmeDirectories.LetsEncrypt)!.RequiresExternalAccountBinding);
        Assert.False(AcmeDirectories.Find(AcmeDirectories.Buypass)!.RequiresExternalAccountBinding);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.ZeroSsl)!.RequiresExternalAccountBinding);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.GoogleTrustServices)!.RequiresExternalAccountBinding);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.SslComRsa)!.RequiresExternalAccountBinding);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.SslComEcc)!.RequiresExternalAccountBinding);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.LetsEncryptStaging)!.IsTestEnvironment);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.BuypassTest)!.IsTestEnvironment);
        Assert.True(AcmeDirectories.Find(AcmeDirectories.GoogleTrustServicesTest)!.IsTestEnvironment);
        Assert.All(AcmeDirectories.KnownCas, ca => Assert.False(string.IsNullOrEmpty(ca.Notes)));
    }

    [Fact]
    public void Find_ReturnsNullForUnknownAndThrowsForNull()
    {
        Assert.Null(AcmeDirectories.Find(new Uri("https://pebble.local:14000/dir")));
        Assert.Throws<ArgumentNullException>(() => AcmeDirectories.Find(null!));
    }
}
