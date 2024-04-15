using System.Reflection;
using System.Runtime.InteropServices;
using Inedo.Extensibility;

[assembly: AssemblyTitle("Amazon AWS")]
[assembly: AssemblyDescription("Contains a ProGet Package Store backed by Amazon S3, and operations for working with S3 storage in Otter and BuildMaster.")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Inedo, LLC")]
[assembly: AssemblyProduct("any")]
[assembly: AssemblyCopyright("Copyright © Inedo 2024")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: ComVisible(false)]

[assembly: AssemblyVersion("3.0.0")]
[assembly: AssemblyFileVersion("3.0.0")]

[assembly: ScriptNamespace("AWS")]
[assembly: AppliesToAttribute (InedoProduct.BuildMaster | InedoProduct.Otter | InedoProduct.ProGet)]
