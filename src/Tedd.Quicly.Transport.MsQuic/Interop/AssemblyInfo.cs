using System.Runtime.CompilerServices;

// ADR 0002: every type crossing the MsQuic boundary is blittable; the runtime marshaller is disabled so
// [LibraryImport] and delegate* unmanaged calls are direct calls with no IL stubs or copies.
[assembly: DisableRuntimeMarshalling]
