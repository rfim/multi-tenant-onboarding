from enum import Enum


class TenantState(str, Enum):
    """
    Ordered states of the onboarding state machine.

    The machine advances linearly from PENDING to ACTIVE. Any handler failure
    transitions directly to FAILED. There is no automatic retry; a human must
    inspect the failure and replay from the failed state using the CLI.
    """

    PENDING = "PENDING"
    REGISTERED = "REGISTERED"
    GRANTS_PROVISIONED = "GRANTS_PROVISIONED"
    PIPELINES_SCAFFOLDED = "PIPELINES_SCAFFOLDED"
    DASHBOARDS_SCAFFOLDED = "DASHBOARDS_SCAFFOLDED"
    QUALITY_SCAFFOLDED = "QUALITY_SCAFFOLDED"
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"

    @classmethod
    def ordered_states(cls) -> list["TenantState"]:
        """Returns the happy-path states in promotion order."""
        return [
            cls.PENDING,
            cls.REGISTERED,
            cls.GRANTS_PROVISIONED,
            cls.PIPELINES_SCAFFOLDED,
            cls.DASHBOARDS_SCAFFOLDED,
            cls.QUALITY_SCAFFOLDED,
            cls.ACTIVE,
        ]

    def next_state(self) -> "TenantState":
        """Returns the next state in the happy path, or raises if terminal."""
        ordered = self.ordered_states()
        if self == TenantState.ACTIVE:
            raise ValueError("ACTIVE is a terminal state; no next state.")
        if self == TenantState.FAILED:
            raise ValueError("FAILED is a terminal state; use replay to resume.")
        idx = ordered.index(self)
        return ordered[idx + 1]

    @property
    def is_terminal(self) -> bool:
        return self in (TenantState.ACTIVE, TenantState.FAILED)
