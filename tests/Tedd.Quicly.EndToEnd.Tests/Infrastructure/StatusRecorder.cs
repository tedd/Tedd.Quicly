using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>Records what a provisioner reports: every status change and every non-fatal error, in order.</summary>
internal sealed class StatusRecorder
{
    private readonly Lock _lock = new();
    private readonly List<CertificateStatus> _statuses = [];
    private readonly List<Exception> _errors = [];

    public StatusRecorder(CertificateProvisioner provisioner)
    {
        provisioner.StatusChanged += status =>
        {
            lock (_lock)
            {
                _statuses.Add(status);
            }
        };
        provisioner.Error += error =>
        {
            lock (_lock)
            {
                _errors.Add(error);
            }
        };
    }

    public CertificateStatus[] Statuses
    {
        get
        {
            lock (_lock)
            {
                return [.. _statuses];
            }
        }
    }

    public Exception[] Errors
    {
        get
        {
            lock (_lock)
            {
                return [.. _errors];
            }
        }
    }

    public bool Has(CertificateState state, string reasonFragment)
    {
        return Statuses.Any(s => s.State == state && s.Reason?.Contains(reasonFragment, StringComparison.Ordinal) == true);
    }

    /// <summary>Every status, one per line, for assertion messages.</summary>
    public string Describe() => string.Join(Environment.NewLine, Statuses.Select(static s => s.ToString()));
}
