namespace Streamlinr;

using ArchUnitNET.Domain;
using ArchUnitNET.Loader;
using ArchUnitNET.xUnitV3;
using Xunit;

using static System.Reflection.Assembly;
using static ArchUnitNET.Fluent.ArchRuleDefinition;

public class PublicApiBoundaryTests {
    static readonly Architecture Architecture = new ArchLoader()
        .LoadAssemblies(Load("Streamlinr"), Load("Streamlinr.Core"), Load("Confluent.Kafka"))
        .Build();

    static (Assembly Streamlinr, Assembly StreamlinrCore, Assembly ConfluentKafka) Assemblies => (
        Architecture.Assemblies.Single(assembly => assembly.Name == "Streamlinr"),
        Architecture.Assemblies.Single(assembly => assembly.Name == "Streamlinr.Core"),
        Architecture.Assemblies.Single(assembly => assembly.Name == "Confluent.Kafka")
    );

    const String CoreBoundaryReason = "Streamlinr.Core is a private implementation assembly. Public Streamlinr APIs must expose Streamlinr-owned public types and map to Streamlinr.Core internally.";

    [Fact]
    public void PublicAssemblyDoesNotDependOnConfluentKafka() {
        var rule = Types()
            .That()
            .ResideInAssembly(Assemblies.Streamlinr)
            .Should()
            .NotDependOnAny(Types().That().ResideInAssembly(Assemblies.ConfluentKafka))
            .Because("Kafka client implementation belongs behind Streamlinr.Core")
            .WithoutRequiringPositiveResults();

        rule.Check(Architecture);
    }

    [Fact]
    public void PublicApiDoesNotExposeCoreImplementationTypes() {
        var typeRule = Types()
            .That()
            .ResideInAssembly(Assemblies.Streamlinr)
            .Should()
            .NotExposeTypesFrom(Types().That().ResideInAssembly(Assemblies.StreamlinrCore))
            .Because(CoreBoundaryReason)
            .WithoutRequiringPositiveResults();

        var methodRule = MethodMembers()
            .That()
            .ArePublic()
            .And()
            .AreDeclaredIn(Types().That().ResideInAssembly(Assemblies.Streamlinr))
            .Should()
            .NotHaveReturnTypes(Types().That().ResideInAssembly(Assemblies.StreamlinrCore))
            .AndShould()
            .NotHaveParameterTypes(Types().That().ResideInAssembly(Assemblies.StreamlinrCore))
            .AndShould()
            .NotExposeGenericConstraintsFrom(Types().That().ResideInAssembly(Assemblies.StreamlinrCore))
            .Because(CoreBoundaryReason)
            .WithoutRequiringPositiveResults();

        var fieldRule = FieldMembers()
            .That()
            .ArePublic()
            .And()
            .AreDeclaredIn(Types().That().ResideInAssembly(Assemblies.Streamlinr))
            .Should()
            .NotHaveFieldType(Types().That().ResideInAssembly(Assemblies.StreamlinrCore))
            .Because(CoreBoundaryReason)
            .WithoutRequiringPositiveResults();

        typeRule.And(methodRule).And(fieldRule).Check(Architecture);
    }
}
