namespace Streamlinr;

using ArchUnitNET.Domain;

static class ArchUnitNetTypeInspection {
    static IEnumerable<IType> FindForbiddenTypes(IType type, ISet<IType> forbiddenTypes) {
        if (forbiddenTypes.Contains(type)) {
            yield return type;
        }

        foreach (var genericArgument in type.GenericParameters.SelectMany(parameter => parameter.TypeConstraints)) {
            foreach (var forbiddenType in FindForbiddenTypes(genericArgument, forbiddenTypes)) {
                yield return forbiddenType;
            }
        }
    }

    static public IEnumerable<IType> FindForbiddenTypes(ITypeInstance<IType> typeInstance, ISet<IType> forbiddenTypes) {
        foreach (var forbiddenType in FindForbiddenTypes(typeInstance.Type, forbiddenTypes)) {
            yield return forbiddenType;
        }

        foreach (var genericArgument in typeInstance.GenericArguments) {
            foreach (var forbiddenType in FindForbiddenTypes(genericArgument, forbiddenTypes)) {
                yield return forbiddenType;
            }
        }
    }
}
