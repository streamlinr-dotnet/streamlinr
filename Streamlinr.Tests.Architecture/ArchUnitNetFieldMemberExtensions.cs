namespace Streamlinr;

using ArchUnitNET.Domain;
using ArchUnitNET.Fluent.Conditions;
using ArchUnitNET.Fluent.Syntax.Elements.Members.FieldMembers;

static public class ArchUnitNetFieldMemberExtensions {
    extension<TNextElement>(IAddFieldMemberCondition<TNextElement, FieldMember> conditions) {
        public TNextElement NotHaveFieldType(IObjectProvider<IType> forbiddenTypes) {
            ArgumentNullException.ThrowIfNull(conditions);
            ArgumentNullException.ThrowIfNull(forbiddenTypes);

            return conditions.FollowCustomCondition(new NotHaveFieldTypeCondition(forbiddenTypes));
        }
    }

    sealed class NotHaveFieldTypeCondition(IObjectProvider<IType> forbiddenTypes) : ICondition<FieldMember> {
        public String Description => $"not have field type {forbiddenTypes.Description}";

        public IEnumerable<ConditionResult> Check(IEnumerable<FieldMember> objects, Architecture architecture) {
            var forbiddenTypeSet = forbiddenTypes
                .GetObjects(architecture)
                .ToHashSet();

            foreach (var field in objects) {
                var forbiddenFieldTypes = ArchUnitNetTypeInspection.FindForbiddenTypes(field, forbiddenTypeSet)
                    .Distinct()
                    .OrderBy(type => type.FullName, StringComparer.Ordinal)
                    .ToArray();

                yield return forbiddenFieldTypes switch {
                    { Length:   0 } => new ConditionResult(analyzedObject: field, pass: true),
                    { Length: > 0 } => new ConditionResult(
                        analyzedObject : field,
                        pass           : false,
                        failDescription: $"has forbidden field type(s) {String.Join(", ", forbiddenFieldTypes.Select(type => $"\"{type.FullName}\""))}\nRepair: {ArchitectureRuleText.GeneralRepair}"
                    ),
                };
            }
        }

        public Boolean CheckEmpty() => true;
    }
}
