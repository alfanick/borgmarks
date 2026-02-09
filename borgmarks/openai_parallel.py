from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Sequence, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def run_batches_adaptive(
    *,
    phase_label: str,
    batches: Sequence[T],
    run_batch: Callable[[int, T], R],
    on_result: Callable[[int, T, R], None],
    on_error: Callable[[int, T, Exception], None],
    min_jobs: int,
    max_jobs: int,
    logger,
) -> None:
    if not batches:
        return

    min_workers = max(1, int(min_jobs))
    max_workers = max(min_workers, int(max_jobs))
    current_workers = min_workers
    start = 0

    while start < len(batches):
        window_size = min(current_workers, len(batches) - start)
        window = [(idx, batches[idx]) for idx in range(start, start + window_size)]
        start += window_size

        window_errors = 0
        with ThreadPoolExecutor(max_workers=window_size) as ex:
            futures = {ex.submit(run_batch, idx, batch): (idx, batch) for idx, batch in window}
            for fut in as_completed(futures):
                idx, batch = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    window_errors += 1
                    on_error(idx, batch, e)
                    continue
                on_result(idx, batch, res)

        if window_errors == 0:
            next_workers = min(max_workers, current_workers * 2)
            if next_workers > current_workers:
                logger.info(
                    "OpenAI %s scaling up workers: %d -> %d",
                    phase_label,
                    current_workers,
                    next_workers,
                )
            current_workers = next_workers
            continue

        next_workers = max(min_workers, max(1, current_workers // 2))
        logger.warning(
            "OpenAI %s window had %d/%d failed batches; scaling down workers: %d -> %d",
            phase_label,
            window_errors,
            len(window),
            current_workers,
            next_workers,
        )
        current_workers = next_workers
