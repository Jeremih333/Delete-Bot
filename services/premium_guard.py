from dataclasses import dataclass


FEATURE_INTERVAL_FAST = "interval_fast"
FEATURE_FROZEN_DELETE = "frozen_delete"
FEATURE_KICK_MODE = "kick_mode"


@dataclass(frozen=True)
class FeatureDecision:
    allowed: bool
    reason_code: str | None = None


def can_use_feature(owner_is_premium: bool, feature: str) -> FeatureDecision:
    if feature in {FEATURE_INTERVAL_FAST, FEATURE_FROZEN_DELETE, FEATURE_KICK_MODE} and not owner_is_premium:
        return FeatureDecision(False, "premium_required")
    return FeatureDecision(True, None)

