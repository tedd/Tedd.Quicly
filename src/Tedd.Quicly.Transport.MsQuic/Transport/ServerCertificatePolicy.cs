using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>SHA-256 pins over a certificate's SubjectPublicKeyInfo (<see cref="ServerCertificateValidationMode.PinnedSpki"/>).</summary>
public static class SpkiPin
{
    /// <summary>The SHA-256 hash of <paramref name="certificate"/>'s DER-encoded SubjectPublicKeyInfo (32 bytes).</summary>
    public static byte[] Compute(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return SHA256.HashData(certificate.PublicKey.ExportSubjectPublicKeyInfo());
    }

    /// <summary>The SHA-256 hash of the SubjectPublicKeyInfo of a DER-encoded certificate (32 bytes).</summary>
    public static byte[] Compute(ReadOnlySpan<byte> certificateDer)
    {
        using X509Certificate2 certificate = X509CertificateLoader.LoadCertificate(certificateDer);
        return Compute(certificate);
    }
}

/// <summary>Client-side certificate decision for one connector (immutable after construction).</summary>
internal sealed class ServerCertificatePolicy
{
    private readonly byte[][] _pins;
    private readonly ServerCertificateValidator? _validator;
    private readonly TransportDiagnosticCallback? _diagnostic;

    public ServerCertificatePolicy(MsQuicTransportOptions options)
    {
        Mode = options.ServerCertificateValidation;
        _pins = new byte[options.PinnedSpkiSha256.Count][];
        for (int i = 0; i < _pins.Length; i++) _pins[i] = (byte[])options.PinnedSpkiSha256[i].Clone();
        _validator = options.ServerCertificateValidator;
        _diagnostic = options.Diagnostic;
    }

    public ServerCertificateValidationMode Mode { get; }

    /// <summary>The wrapper credential mode implementing <see cref="Mode"/>: pins and callbacks both use deferred portable validation.</summary>
    public MsQuicCertificateValidation WrapperValidation => Mode switch
    {
        ServerCertificateValidationMode.SystemRoots => MsQuicCertificateValidation.SystemRoots,
        ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate => MsQuicCertificateValidation.InsecureSkipValidation,
        _ => MsQuicCertificateValidation.Callback,
    };

    /// <summary>Decides on a certificate indicated through <c>PEER_CERTIFICATE_RECEIVED</c>. Never throws.</summary>
    public MsQuicCertificateDecision Decide(in MsQuicPeerCertificateInfo info, string? serverName)
    {
        try
        {
            bool platformValid = info.DeferredStatus == MsQuicStatus.QUIC_STATUS_SUCCESS && info.DeferredErrorFlags == 0;
            switch (Mode)
            {
                case ServerCertificateValidationMode.PinnedSpki:
                    return MatchesPin(info.CertificateDer) ? MsQuicCertificateDecision.Accept : Reject("the server certificate's SPKI matches no pin");
                case ServerCertificateValidationMode.Callback:
                    var context = new ServerCertificateContext
                    {
                        LeafDer = info.CertificateDer,
                        ChainPkcs7 = info.ChainPkcs7,
                        PlatformValid = platformValid,
                        PlatformStatus = info.DeferredStatus,
                        ServerName = serverName,
                    };
                    return _validator!(in context) switch
                    {
                        ServerCertificateDecision.AcceptIgnoringPlatformValidation => MsQuicCertificateDecision.Accept,
                        ServerCertificateDecision.Accept when platformValid => MsQuicCertificateDecision.Accept,
                        ServerCertificateDecision.Accept => Reject("the validator accepted but platform validation failed with " + MsQuicStatus.GetName(info.DeferredStatus)),
                        _ => Reject("the validator rejected the server certificate"),
                    };
                case ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate:
                    return MsQuicCertificateDecision.Accept;
                default:
                    return platformValid ? MsQuicCertificateDecision.Accept : MsQuicCertificateDecision.Reject;
            }
        }
        catch (Exception ex)
        {
            Diagnose(TransportDiagnosticLevel.Error, "Server certificate validation threw; the certificate is rejected.", ex);
            return MsQuicCertificateDecision.Reject;
        }
    }

    private bool MatchesPin(ReadOnlySpan<byte> leafDer)
    {
        if (leafDer.IsEmpty) return false;
        byte[] hash = SpkiPin.Compute(leafDer);
        bool match = false;
        foreach (byte[] pin in _pins) match |= CryptographicOperations.FixedTimeEquals(pin, hash);
        return match;
    }

    private MsQuicCertificateDecision Reject(string why)
    {
        Diagnose(TransportDiagnosticLevel.Warning, "Server certificate rejected: " + why + ".", null);
        return MsQuicCertificateDecision.Reject;
    }

    /// <summary>Reports to the user's diagnostic callback; an exception it throws is swallowed (the policy never throws).</summary>
    private void Diagnose(TransportDiagnosticLevel level, string message, Exception? exception)
    {
        try
        {
            _diagnostic?.Invoke(level, message, exception);
        }
        catch
        {
            // A diagnostics sink must not break certificate validation.
        }
    }
}
