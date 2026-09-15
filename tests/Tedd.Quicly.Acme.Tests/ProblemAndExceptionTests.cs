using System.Net;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme.Tests;

public class ProblemAndExceptionTests
{
    [Fact]
    public void Problem_ToString_IncludesStatusDetailAndSubproblems()
    {
        AcmeProblem problem = new()
        {
            Type = AcmeErrorTypes.Compound,
            Detail = "Some identifiers were rejected",
            Status = 400,
            Subproblems =
            [
                new AcmeProblem { Type = AcmeErrorTypes.RejectedIdentifier, Detail = "policy", Identifier = AcmeIdentifier.Dns("bad.test") },
                new AcmeProblem { Type = AcmeErrorTypes.Dns, Detail = "no records" },
            ],
        };
        string text = problem.ToString();
        Assert.Equal("urn:ietf:params:acme:error:compound (400): Some identifiers were rejected [bad.test: urn:ietf:params:acme:error:rejectedIdentifier: policy; urn:ietf:params:acme:error:dns: no records]", text);
        Assert.True(problem.IsType(AcmeErrorTypes.Compound));
        Assert.False(problem.IsType(AcmeErrorTypes.Dns));
    }

    [Fact]
    public void Problem_ToString_MinimalAndEmptySubproblems()
    {
        Assert.Equal(AcmeErrorTypes.Unknown, new AcmeProblem().ToString());
        Assert.Equal("t", new AcmeProblem { Type = "t", Subproblems = [] }.ToString());
    }

    [Fact]
    public void Exception_ExposesProblemAndStatus()
    {
        AcmeProblem problem = new() { Type = AcmeErrorTypes.BadNonce, Detail = "stale" };
        AcmeException e = new(problem, HttpStatusCode.BadRequest) { RetryAfter = TimeSpan.FromSeconds(3) };
        Assert.Same(problem, e.Problem);
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(AcmeErrorTypes.BadNonce, e.Type);
        Assert.True(e.IsType(AcmeErrorTypes.BadNonce));
        Assert.Equal(TimeSpan.FromSeconds(3), e.RetryAfter);
        Assert.Equal(problem.ToString(), e.Message);

        AcmeException nullProblem = new(null!);
        Assert.Equal(AcmeErrorTypes.Unknown, nullProblem.Type);
        Assert.Null(nullProblem.StatusCode);

        InvalidOperationException inner = new("inner");
        AcmeException client = AcmeException.Client(AcmeErrorTypes.PollTimeout, "took too long", inner);
        Assert.Same(inner, client.InnerException);
        Assert.Equal("took too long", client.Problem.Detail);
        Assert.Null(client.StatusCode);
    }
}
