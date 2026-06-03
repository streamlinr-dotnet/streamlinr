namespace Streamlinr;

static class ArchitectureRuleText {
    public const String GeneralRepair = "Replace the exposed Streamlinr.Core type with a Streamlinr-owned public type and translate internally before calling Streamlinr.Core.";

    public const String InheritanceRepair = "Do not inherit from or implement Streamlinr.Core types in public Streamlinr APIs; introduce a Streamlinr-owned public base/interface if one is required.";

    public const String ConstraintRepair = "Remove the Streamlinr.Core generic constraint from the public API; constrain on a Streamlinr-owned public abstraction or perform validation inside Streamlinr.";

    public const String AttributeRepair = "Do not use Streamlinr.Core attributes on public API surfaces; define any public metadata attribute in Streamlinr.";
}
