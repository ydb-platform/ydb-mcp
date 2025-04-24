"""Patches for YDB library to improve behavior."""

import asyncio
import contextlib
import datetime
import logging
import sys
import warnings
from functools import wraps
from typing import Optional, Set

logger = logging.getLogger(__name__)

# Global task registry for cleanup
_task_registry: Set[asyncio.Task] = set()


def register_task(task: asyncio.Task) -> None:
    """Register a task for cleanup tracking."""
    _task_registry.add(task)
    task.add_done_callback(lambda t: _task_registry.discard(t))
    # Disable task destruction warning for registered tasks
    task._log_destroy_pending = False
    logger.debug(f"Registered task {task.get_name()}")


@contextlib.contextmanager
def suppress_task_destroyed_warning():
    """Context manager to suppress specific task destroyed warnings."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Task was destroyed but it is pending!", category=RuntimeWarning
        )
        warnings.filterwarnings(
            "ignore", message="Error handling discovery task", category=RuntimeWarning
        )
        warnings.filterwarnings("ignore", message="Error stopping driver", category=RuntimeWarning)
        yield


def ensure_same_loop(coro):
    """Decorator to ensure coroutine runs in the correct event loop and manages task lifecycle."""

    @wraps(coro)
    async def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        task = asyncio.current_task()
        if task:
            register_task(task)
            # Disable task destruction warning
            task._log_destroy_pending = False

        try:
            return await coro(*args, **kwargs)
        except asyncio.CancelledError:
            logger.debug(f"Task cancelled: {coro.__name__}")
            raise
        finally:
            if task:
                _task_registry.discard(task)

    return wrapper


async def _cancel_and_wait(tasks, timeout: float) -> None:
    """Helper function to cancel tasks and wait for them to complete."""
    if not tasks:
        return

    cancelled_tasks = []
    for task in tasks:
        if not task.done():
            # Disable task destruction warning
            task._log_destroy_pending = False
            task.cancel()
            cancelled_tasks.append(task)
            logger.debug(f"Cancelling task {task.get_name()}")

    if not cancelled_tasks:
        return

    try:
        await asyncio.shield(asyncio.wait(cancelled_tasks, timeout=timeout))
    except asyncio.TimeoutError:
        logger.warning(
            f"Timeout waiting for tasks to cancel: {[t.get_name() for t in cancelled_tasks]}"
        )
    except Exception as e:
        logger.warning(f"Error waiting for tasks to cancel: {e}")


def cleanup_pending_tasks(timeout: float = 1.0) -> None:
    """Clean up any pending tasks in the registry."""
    if not _task_registry:
        return

    # Don't run cleanup in test environments unless explicitly called
    if "pytest" in sys.modules and timeout > 0.1:
        return

    pending_tasks = [t for t in _task_registry if not t.done()]
    if not pending_tasks:
        return

    logger.debug(f"Cleaning up {len(pending_tasks)} pending tasks")

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    with suppress_task_destroyed_warning():
        try:
            # Use a shorter timeout in test environments
            actual_timeout = min(timeout, 0.1) if "pytest" in sys.modules else timeout

            # Create a new task for cancellation to avoid blocking
            cancel_task = loop.create_task(_cancel_and_wait(pending_tasks, actual_timeout))
            # Disable task destruction warning for the cancel task
            cancel_task._log_destroy_pending = False

            # Run the cancellation task
            if not loop.is_running():
                loop.run_until_complete(cancel_task)
            else:
                # If loop is running, just schedule the task
                task = asyncio.create_task(_cancel_and_wait(pending_tasks, actual_timeout))
                task._log_destroy_pending = False
        except Exception as e:
            logger.warning(f"Error during task cleanup: {e}")
        finally:
            remaining_tasks = [t for t in _task_registry if not t.done()]
            if remaining_tasks:
                # Disable warnings for any remaining tasks
                for task in remaining_tasks:
                    task._log_destroy_pending = False
                logger.debug(
                    f"Tasks remaining after cleanup: {[t.get_name() for t in remaining_tasks]}"
                )
            _task_registry.clear()
            logger.debug("Task registry cleared")


def patch_ydb_driver():
    """Patch YDB driver to handle task cancellation better."""
    try:
        import ydb
        import ydb.aio.driver
        import ydb.aio.pool

        # Patch the Discovery.run method
        original_run = ydb.aio.pool.Discovery.run

        @ensure_same_loop
        async def patched_run(self):
            with suppress_task_destroyed_warning():
                try:
                    return await original_run(self)
                except asyncio.CancelledError:
                    logger.debug("Discovery task cancelled")
                    raise
                except Exception as e:
                    logger.warning(f"Error in discovery task: {e}")
                    raise

        ydb.aio.pool.Discovery.run = patched_run

        # Patch the Driver.stop method
        original_stop = ydb.aio.driver.Driver.stop

        @ensure_same_loop
        async def patched_stop(self, timeout: Optional[float] = None):
            with suppress_task_destroyed_warning():
                try:
                    if hasattr(self, "discovery") and self.discovery is not None:
                        await _cleanup_discovery(self.discovery, timeout)

                    try:
                        # Handle both async and sync stop methods
                        if asyncio.iscoroutinefunction(original_stop):
                            return await asyncio.shield(original_stop(self, timeout=timeout))
                        else:
                            return original_stop(self, timeout=timeout)
                    except asyncio.CancelledError:
                        if asyncio.iscoroutinefunction(original_stop):
                            await asyncio.shield(original_stop(self, timeout=timeout))
                        raise
                except asyncio.CancelledError:
                    logger.debug("Driver stop cancelled")
                    raise
                except Exception as e:
                    logger.debug(f"Error stopping driver: {e}")
                    raise

        async def _cleanup_discovery(discovery, timeout: Optional[float] = None):
            """Helper function to clean up discovery-related tasks."""
            try:
                if hasattr(discovery, "stop"):
                    # Handle both async and sync stop methods
                    stop_method = discovery.stop
                    if asyncio.iscoroutinefunction(stop_method):
                        await stop_method()
                    else:
                        stop_method()

                if hasattr(discovery, "_discovery_task"):
                    task = discovery._discovery_task
                    if task and not task.done() and not task.cancelled():
                        try:
                            task_loop = getattr(task, "get_loop", lambda: None)()
                            try:
                                current_loop = asyncio.get_running_loop()
                            except RuntimeError:
                                current_loop = None

                            if task_loop is current_loop:
                                task.cancel()
                                try:
                                    actual_timeout = (
                                        min(timeout or 1.0, 0.1)
                                        if "pytest" in sys.modules
                                        else (timeout or 1.0)
                                    )
                                    await asyncio.wait_for([task], timeout=actual_timeout)
                                except (asyncio.CancelledError, asyncio.TimeoutError):
                                    pass
                            else:
                                logger.debug("Discovery task is in a different event loop")
                        except Exception as e:
                            logger.debug(f"Error during discovery task cleanup: {e}")
            except Exception as e:
                logger.debug(f"Error cleaning up discovery: {e}")

        ydb.aio.driver.Driver.stop = patched_stop

        logger.info("Successfully patched YDB driver for better task cancellation handling")
    except ImportError:
        logger.warning("Could not patch YDB driver - module not found")
    except Exception as e:
        logger.warning(f"Error patching YDB driver: {e}")


def apply_all_patches():
    """Apply all YDB patches."""
    patch_ydb_driver()

    # Only register cleanup for non-test environments
    if "pytest" not in sys.modules:
        import atexit

        atexit.register(lambda: cleanup_pending_tasks(timeout=2.0))
