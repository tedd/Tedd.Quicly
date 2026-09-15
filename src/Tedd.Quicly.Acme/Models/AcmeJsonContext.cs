using System.Text.Json;
using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>Source-generated serializer context for every JSON document exchanged with an ACME server (AOT-safe).</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    PropertyNameCaseInsensitive = true,
    WriteIndented = false)]
[JsonSerializable(typeof(AcmeDirectory))]
[JsonSerializable(typeof(AcmeAccount))]
[JsonSerializable(typeof(AcmeOrder))]
[JsonSerializable(typeof(AcmeAuthorization))]
[JsonSerializable(typeof(AcmeChallenge))]
[JsonSerializable(typeof(AcmeProblem))]
[JsonSerializable(typeof(AcmeIdentifier))]
[JsonSerializable(typeof(Jwk))]
[JsonSerializable(typeof(JwsEnvelope))]
[JsonSerializable(typeof(JwsProtectedHeader))]
[JsonSerializable(typeof(NewAccountRequest))]
[JsonSerializable(typeof(AccountUpdateRequest))]
[JsonSerializable(typeof(NewOrderRequest))]
[JsonSerializable(typeof(FinalizeRequest))]
[JsonSerializable(typeof(RevokeRequest))]
[JsonSerializable(typeof(EmptyRequest))]
[JsonSerializable(typeof(AcmeAccountState))]
[JsonSerializable(typeof(AcmePendingOrder))]
[JsonSerializable(typeof(AcmeStoreEnvelope))]
[JsonSerializable(typeof(AcmeRenewalInfo))]
internal sealed partial class AcmeJsonContext : JsonSerializerContext
{
}
