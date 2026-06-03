namespace Streamlinr;

using ArchUnitNET.Domain;
using ArchUnitNET.Fluent.Conditions;
using ArchUnitNET.Fluent.Syntax.Elements.Types;

static public class ArchUnitNetTypeExtensions {
    extension<TNextElement>(IAddTypeCondition<TNextElement, IType> conditions) {
        public TNextElement NotExposeTypesFrom(IObjectProvider<IType> forbiddenTypes) {
            ArgumentNullException.ThrowIfNull(conditions);
            ArgumentNullException.ThrowIfNull(forbiddenTypes);

            return conditions.FollowCustomCondition(new NotExposeTypesCondition(forbiddenTypes));
        }
    }

    sealed class NotExposeTypesCondition(IObjectProvider<IType> forbiddenTypes) : ICondition<IType> {
        public String Description => $"not expose types {forbiddenTypes.Description} through public inheritance, interfaces, constraints, or attributes";

        public IEnumerable<ConditionResult> Check(IEnumerable<IType> objects, Architecture architecture) {
            var forbiddenTypeSet = forbiddenTypes.GetObjects(architecture).ToHashSet();

            foreach (var type in objects) {
                foreach (var violation in FindViolations(type, forbiddenTypeSet)) {
                    yield return new ConditionResult(type, pass: false, violation);
                }

                yield return new ConditionResult(type, pass: true);
            }
        }

        public Boolean CheckEmpty() => true;

        static IEnumerable<String> FindViolations(IType type, ISet<IType> forbiddenTypes) {
            foreach (var attribute in type.Attributes.Where(forbiddenTypes.Contains)) {
                yield return FormatViolation("public attribute", type, attribute, ArchitectureRuleText.AttributeRepair, $"uses attribute [{attribute.FullName}]");
            }

            if (type is Class classType) {
                foreach (var inheritedClass in classType.InheritedClasses.Where(forbiddenTypes.Contains)) {
                    yield return FormatViolation("public inheritance", type, inheritedClass, ArchitectureRuleText.InheritanceRepair, $"inherits from {inheritedClass.FullName}");
                }
            }

            foreach (var implementedInterface in type.ImplementedInterfaces.Where(forbiddenTypes.Contains)) {
                yield return FormatViolation("public interface implementation", type, implementedInterface, ArchitectureRuleText.InheritanceRepair, $"implements {implementedInterface.FullName}");
            }

            foreach (var genericParameter in type.GenericParameters) {
                foreach (var constraint in genericParameter.TypeConstraints.Where(forbiddenTypes.Contains)) {
                    yield return FormatViolation("public generic type constraint", type, constraint, ArchitectureRuleText.ConstraintRepair, $"generic parameter {genericParameter.Name} constrained to {constraint.FullName}");
                }
            }
        }

        static String FormatViolation(String kind, IType type, IType forbiddenType, String repair, String details) =>
            $"{kind}: public type {type.FullName} {details}\nForbidden type: {forbiddenType.FullName}\nRepair: {repair}";
    }
}
