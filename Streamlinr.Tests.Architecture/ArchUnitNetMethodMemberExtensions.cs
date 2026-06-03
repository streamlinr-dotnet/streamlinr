namespace Streamlinr;

using ArchUnitNET.Domain;
using ArchUnitNET.Fluent.Conditions;
using ArchUnitNET.Fluent.Syntax.Elements.Members.MethodMembers;

static public class ArchUnitNetMethodMemberExtensions {
    extension<TNextElement>(IAddMethodMemberCondition<TNextElement, MethodMember> conditions) {
        public TNextElement NotHaveParameterTypes(IObjectProvider<IType> forbiddenTypes) {
            ArgumentNullException.ThrowIfNull(conditions);
            ArgumentNullException.ThrowIfNull(forbiddenTypes);

            return conditions.FollowCustomCondition(new NotHaveParameterTypesCondition(forbiddenTypes));
        }

        public TNextElement NotHaveReturnTypes(IObjectProvider<IType> forbiddenTypes) {
            ArgumentNullException.ThrowIfNull(conditions);
            ArgumentNullException.ThrowIfNull(forbiddenTypes);

            return conditions.FollowCustomCondition(new NotHaveReturnTypesCondition(forbiddenTypes));
        }

        public TNextElement NotExposeGenericConstraintsFrom(IObjectProvider<IType> forbiddenTypes) {
            ArgumentNullException.ThrowIfNull(conditions);
            ArgumentNullException.ThrowIfNull(forbiddenTypes);

            return conditions.FollowCustomCondition(new NotExposeGenericConstraintsCondition(forbiddenTypes));
        }
    }

    class NotHaveParameterTypesCondition(IObjectProvider<IType> forbiddenTypes) : ICondition<MethodMember> {
        public String Description => $"not have parameter types {forbiddenTypes.Description}";

        public IEnumerable<ConditionResult> Check(IEnumerable<MethodMember> objects, Architecture architecture) {
            var forbiddenTypeSet = forbiddenTypes
                .GetObjects(architecture)
                .ToHashSet();

            foreach (var method in objects) {
                var forbiddenParameters = method.ParameterInstances
                    .SelectMany(parameter => ArchUnitNetTypeInspection.FindForbiddenTypes(parameter, forbiddenTypeSet))
                    .Distinct()
                    .OrderBy(type => type.FullName, StringComparer.Ordinal)
                    .ToArray();

                yield return forbiddenParameters switch {
                    { Length: 0   } => new ConditionResult(analyzedObject: method, pass: true),
                    { Length: > 0 } => new ConditionResult(
                        analyzedObject : method,
                        pass           : false,
                        failDescription: $"has forbidden parameter type(s) {String.Join(", ", forbiddenParameters.Select(type => $"\"{type.FullName}\""))}\nRepair: {ArchitectureRuleText.GeneralRepair}"
                    ),
                };
            }
        }

        public Boolean CheckEmpty() => true;
    }

    class NotHaveReturnTypesCondition(IObjectProvider<IType> forbiddenTypes) : ICondition<MethodMember> {
        public String Description => $"not have return types {forbiddenTypes.Description}";

        public IEnumerable<ConditionResult> Check(IEnumerable<MethodMember> objects, Architecture architecture) {
            var forbiddenTypeSet = forbiddenTypes.GetObjects(architecture).ToHashSet();

            foreach (var method in objects) {
                var forbiddenReturnTypes = ArchUnitNetTypeInspection.FindForbiddenTypes(method.ReturnTypeInstance, forbiddenTypeSet)
                    .Distinct()
                    .OrderBy(type => type.FullName, StringComparer.Ordinal)
                    .ToArray();

                yield return forbiddenReturnTypes switch {
                    { Length: 0   } => new ConditionResult(analyzedObject: method, pass: true),
                    { Length: > 0 } => new ConditionResult(
                        analyzedObject : method,
                        pass           : false,
                        failDescription: $"has forbidden return type(s) {String.Join(", ", forbiddenReturnTypes.Select(type => $"\"{type.FullName}\""))}\nRepair: {ArchitectureRuleText.GeneralRepair}"
                    ),
                };
            }
        }

        public Boolean CheckEmpty() => true;
    }

    class NotExposeGenericConstraintsCondition(IObjectProvider<IType> forbiddenTypes) : ICondition<MethodMember> {
        public String Description => $"not expose generic constraints {forbiddenTypes.Description}";

        public IEnumerable<ConditionResult> Check(IEnumerable<MethodMember> objects, Architecture architecture) {
            var forbiddenTypeSet = forbiddenTypes.GetObjects(architecture).ToHashSet();

            foreach (var method in objects) {
                var forbiddenConstraints = method.GenericParameters
                    .SelectMany(parameter => parameter.TypeConstraints)
                    .Where(forbiddenTypeSet.Contains)
                    .OrderBy(type => type.FullName, StringComparer.Ordinal)
                    .ToArray();

                yield return forbiddenConstraints switch {
                    { Length: 0   } => new ConditionResult(analyzedObject: method, pass: true),
                    { Length: > 0 } => new ConditionResult(
                        analyzedObject : method,
                        pass           : false,
                        failDescription: $"has forbidden generic constraint(s) {String.Join(", ", forbiddenConstraints.Select(type => $"\"{type.FullName}\""))}\nRepair: {ArchitectureRuleText.ConstraintRepair}"
                    ),
                };
            }
        }

        public Boolean CheckEmpty() => true;
    }
}
