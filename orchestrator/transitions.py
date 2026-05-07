from orchestrator.states import TenantState

# Legal transitions: maps (from_state, to_state) -> allowed
# The state machine only uses forward transitions and the FAILED escape hatch.
# Backward transitions are not permitted; replay starts from the failed state.

LEGAL_TRANSITIONS: dict[tuple[TenantState, TenantState], bool] = {
    (TenantState.PENDING,               TenantState.REGISTERED):            True,
    (TenantState.REGISTERED,            TenantState.GRANTS_PROVISIONED):    True,
    (TenantState.GRANTS_PROVISIONED,    TenantState.PIPELINES_SCAFFOLDED):  True,
    (TenantState.PIPELINES_SCAFFOLDED,  TenantState.DASHBOARDS_SCAFFOLDED): True,
    (TenantState.DASHBOARDS_SCAFFOLDED, TenantState.QUALITY_SCAFFOLDED):    True,
    (TenantState.QUALITY_SCAFFOLDED,    TenantState.ACTIVE):                True,
    # Any non-terminal state may transition to FAILED
    (TenantState.PENDING,               TenantState.FAILED):                True,
    (TenantState.REGISTERED,            TenantState.FAILED):                True,
    (TenantState.GRANTS_PROVISIONED,    TenantState.FAILED):                True,
    (TenantState.PIPELINES_SCAFFOLDED,  TenantState.FAILED):                True,
    (TenantState.DASHBOARDS_SCAFFOLDED, TenantState.FAILED):                True,
    (TenantState.QUALITY_SCAFFOLDED,    TenantState.FAILED):                True,
}


def assert_legal(from_state: TenantState, to_state: TenantState) -> None:
    """Raises ValueError if the transition is not permitted."""
    if not LEGAL_TRANSITIONS.get((from_state, to_state), False):
        raise ValueError(
            f"Illegal state transition: {from_state.value} -> {to_state.value}"
        )
