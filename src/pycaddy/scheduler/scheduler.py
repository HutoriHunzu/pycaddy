from time import monotonic
from typing import Callable, Any


class _Task:
    def __init__(self, func: Callable[[], Any], period_seconds: float) -> None:
        if period_seconds <= 0:
            raise ValueError(f"period must be > 0, got {period_seconds!r}")
        self.func = func
        self.period_seconds = period_seconds
        self.accumulate_seconds = 0.0  # time since last run

    def is_due(self) -> bool:
        return self.accumulate_seconds >= self.period_seconds

    def reset(self) -> None:
        self.accumulate_seconds = 0.0

    def advance(self, delta_s: float) -> None:
        self.accumulate_seconds += delta_s

    def overshoot(self) -> float:
        return self.accumulate_seconds - self.period_seconds


class PeriodicScheduler:
    """
    Manual, on-demand periodic scheduler.
    Nothing happens until you call `check_and_execute()`.  At that call it:

    - measures how much real time has passed since the last check,
    - adds that elapsed time to every task’s internal timer,
    - then, in a loop:
      - finds the task that is most overdue,
      - runs it once,
      - resets its timer to zero,
      - adds the actual runtime of that task to every other task’s timer,
    - stops when no task’s timer has reached its period.
    """

    def __init__(self) -> None:
        self._last_check = monotonic()
        self._tasks: list[_Task] = []

    def add_task(self, func: Callable[[], Any], period_s: float) -> None:
        """
        Schedule `func()` to run once every `period_s` seconds.
        If enough time has already passed by the next check, it will
        run immediately on that first call to `check_and_execute()`.
        """
        self._tasks.append(_Task(func, period_s))

    def check_and_execute(self) -> None:
        """
        Advance the internal clock, then run each task that’s due,
        one at a time, in order of who’s most overdue.
        Each task’s actual runtime is counted toward every other
        task’s wait time so that slow tasks don’t “steal” from fast ones.
        """
        if not self._tasks:
            return

        now = monotonic()
        elapsed = now - self._last_check
        self._last_check = now

        # age all tasks by real elapsed time
        for task in self._tasks:
            task.advance(elapsed)

        min_period = min(t.period_seconds for t in self._tasks)

        while True:
            # pick only those whose timer >= their period
            due = [t for t in self._tasks if t.is_due()]
            if not due:
                break

            # most overdue first
            task = max(due, key=lambda t: t.overshoot())

            start = monotonic()
            try:
                task.func()
            except Exception as e:
                print(f"Error in scheduled task: {e!r}")
            duration = monotonic() - start

            task.reset()

            # everyone else ages while this one ran
            for other in self._tasks:
                if other is not task:
                    other.advance(duration)

            if duration > min_period:
                raise ValueError(
                    f"Task took {duration:.3f}s but the shortest "
                    f"period is {min_period:.3f}s - it will re-trigger itself!"
                )
