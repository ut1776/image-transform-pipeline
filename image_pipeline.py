"""PixelFlow: concurrent image transformation pipeline.

Applies an ordered chain of transforms to each image, runs independent images
concurrently, and stays within a worker limit and an input-byte admission budget.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

Transform = Callable[[bytes], bytes]


@dataclass(frozen=True)
class ImageJob:
    image_id: str
    data: bytes


@dataclass(frozen=True)
class TransformResult:
    index: int
    image_id: str
    data: bytes | None
    error: str | None


class ImagePipeline:
    def __init__(self, max_workers: int = 4, memory_limit_bytes: int = 16_000_000):
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if memory_limit_bytes < 1:
            raise ValueError("memory_limit_bytes must be positive")

        # Maximum number of image jobs that may execute concurrently.
        self.max_workers = max_workers

        # Maximum admitted input bytes allowed at the same time.
        self.memory_limit_bytes = memory_limit_bytes

    @staticmethod
    def _run_one(
        index: int,
        job: ImageJob,
        transforms: Sequence[Transform],
    ) -> TransformResult:
        """Apply all transforms to one image and return its result."""
        data = job.data
        try:
            for transform in transforms:
                data = transform(data)
        except Exception as exc:  # BaseException is deliberately not caught
            return TransformResult(
                index=index,
                image_id=job.image_id,
                data=None,
                error=f"{type(exc).__name__}: {exc}",
            )
        return TransformResult(
            index=index, image_id=job.image_id, data=data, error=None
        )

    def run(
        self,
        jobs: Iterable[ImageJob],
        transforms: Sequence[Transform],
    ) -> list[TransformResult]:
        """Process all jobs according to the ordering, concurrency, and memory contract."""
        source = iter(jobs)
        results: dict[int, TransformResult] = {}

        # Submitted futures mapped to the input bytes they are charged.
        in_flight: dict = {}
        in_flight_bytes = 0

        # Invariants:
        #   * next_index counts inputs fetched from `source` so far.
        #   * `head` is the fetched-but-not-yet-admitted job (FIFO: never skipped).
        #   * `results` is incomplete until every in-flight future has drained.
        head = None
        next_index = 0
        exhausted = False

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while True:
                # Lazily fetch the head only when a worker slot is free.
                if head is None and not exhausted and len(in_flight) < self.max_workers:
                    try:
                        head = (next_index, next(source))
                        next_index += 1
                    except StopIteration:
                        exhausted = True

                # Admit in input order while there is a free slot.
                while head is not None and len(in_flight) < self.max_workers:
                    index, job = head
                    size = len(job.data)
                    fits = in_flight_bytes + size <= self.memory_limit_bytes
                    # An oversized head may still run, but only when nothing
                    # else is in flight.
                    if not fits and in_flight:
                        break
                    fut = pool.submit(self._run_one, index, job, transforms)
                    in_flight[fut] = size
                    in_flight_bytes += size
                    head = None
                    if len(in_flight) < self.max_workers:
                        try:
                            head = (next_index, next(source))
                            next_index += 1
                        except StopIteration:
                            exhausted = True
                            break

                if not in_flight:
                    if head is None and exhausted:
                        break
                    continue

                # Block until at least one job finishes, then release its bytes.
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight_bytes -= in_flight.pop(fut)
                    res = fut.result()
                    results[res.index] = res

        return [results[i] for i in range(next_index)]
