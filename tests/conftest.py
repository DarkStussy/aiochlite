import os

import pytest

_INTEGRATION_MARKER = f"tests{os.sep}integration{os.sep}"


def _is_integration(nodeid: str) -> bool:
    return _INTEGRATION_MARKER in nodeid.replace("/", os.sep)


def _state(session: pytest.Session) -> dict[str, bool]:
    state = getattr(session, "_core_state", None)
    if state is None:
        state = {"core_failed": False}
        setattr(session, "_core_state", state)
    return state


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]):
    """Run core tests first by moving integration tests to the end."""
    _state(session)  # ensure state is initialized
    items.sort(key=lambda item: _is_integration(item.nodeid))


def pytest_runtest_setup(item: pytest.Item):
    if _state(item.session)["core_failed"] and _is_integration(item.nodeid):
        pytest.skip("Skipping integration tests because core tests failed")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()

    if (
        report.failed
        and report.when == "call"
        and not getattr(report, "wasxfail", False)
        and not _is_integration(report.nodeid)
    ):
        _state(item.session)["core_failed"] = True
