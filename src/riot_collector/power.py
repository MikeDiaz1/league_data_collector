"""Windows idle-sleep protection scoped to the collector's main thread."""
from contextlib import contextmanager
import logging
import os


LOG = logging.getLogger(__name__)
IS_WINDOWS = os.name == "nt"
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


def _set_execution_state(flags: int) -> int:
    import ctypes
    from ctypes import wintypes

    function = ctypes.WinDLL("kernel32", use_last_error=True).SetThreadExecutionState
    function.argtypes = (wintypes.DWORD,)
    function.restype = wintypes.DWORD
    return function(flags)


@contextmanager
def keep_awake():
    """Prevent automatic system sleep; leave display and power-plan settings alone.

    Windows releases this thread-owned request if the process exits or crashes.
    Explicit user sleep, lid actions and shutdown still take precedence.
    """
    if not IS_WINDOWS:
        yield
        return
    previous = _set_execution_state(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    if not previous:
        raise OSError("Windows refused the collector's automatic-sleep prevention request")
    LOG.info("Windows automatic-sleep prevention active for this collector; display may turn off")
    try:
        yield
    finally:
        if _set_execution_state(ES_CONTINUOUS | previous):
            LOG.info("Windows automatic-sleep prevention released")
        else:
            LOG.warning("Could not restore thread power state; Windows releases it when this process exits")
