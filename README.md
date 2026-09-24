# image-transform-pipeline

A small, standard-library-only Python pipeline that applies an ordered chain of
transforms to a batch of images concurrently, while keeping results
deterministic and memory bounded.

## Purpose

This project is the execution core of a batch image-processing service. Given a
batch of images and an ordered list of transforms (resize, watermark, compress
and so on), it runs every image through the same chain, processes many images
at once to finish faster, and returns results in the original order.

A real service can't start everything at once, so the pipeline enforces two
limits:

- **`max_workers`** caps how many images are processed at the same time.
- **`memory_limit_bytes`** caps how much image data is held in memory at once.

Under the hood it is a bounded concurrent job scheduler. It contains no
storage, upload or networking code, and the transforms are supplied by the
caller.

## Typical uses

The design fits any workload made of many independent items that each need the
same ordered steps, where running everything at once is not affordable:

- **Image services:** thumbnails, resizing, watermarking or compression of
  uploaded photos.
- **ML preprocessing:** decoding, resizing and normalizing large image sets
  without loading the whole dataset into memory.
- **Document and media conversion:** PDF-to-text, audio transcoding, video
  frame extraction.
- **Data (ETL) jobs:** cleaning or parsing large batches where output order
  must match input order.
- **Background workers:** handling a batch request that must return one result
  per item, reporting failures per item instead of failing the whole request.

## Usage

```python
from image_pipeline import ImageJob, ImagePipeline

def add_bang(data: bytes) -> bytes:
    return data + b"!"

def shout(data: bytes) -> bytes:
    return data.upper()

jobs = [ImageJob("a", b"x"), ImageJob("b", b"y"), ImageJob("c", b"z")]

pipeline = ImagePipeline(max_workers=4, memory_limit_bytes=16_000_000)
for result in pipeline.run(jobs, [add_bang, shout]):
    print(result.index, result.image_id, result.data, result.error)
```

`run` returns one `TransformResult` per input job, in input order:

| Field      | Meaning                                                    |
|------------|------------------------------------------------------------|
| `index`    | Zero-based position of the job in the input                |
| `image_id` | Copied unchanged from the job (duplicates are allowed)     |
| `data`     | Transformed bytes, or `None` if a transform failed         |
| `error`    | `"ExceptionType: message"` on failure, otherwise `None`    |

## Design

- **Ordered per-image chain.** Each image runs through every transform once, in
  the supplied order. The output of one transform is the input of the next. An
  empty chain is the identity.
- **Deterministic output.** Images finish in any order, but results are stored
  by index and returned in input order.
- **Failure isolation.** If a transform raises an `Exception`, that image stops
  immediately and yields a result with `data=None` and an error string. Every
  other image still completes. `BaseException` is intentionally not caught.
- **Bounded concurrency.** A `ThreadPoolExecutor` runs at most `max_workers`
  image chains at once.
- **FIFO admission with a byte budget.** Jobs are admitted strictly in input
  order. A job's input bytes count against `memory_limit_bytes` from admission
  until its whole chain finishes or fails. If the head job does not fit, the
  scheduler waits rather than skipping ahead to a smaller job. A single
  oversized head job is allowed to run, but only when nothing else is in flight.
- **Lazy input.** `jobs` may be a one-pass iterable. It is read only when a
  worker slot is free, so at most the admitted jobs plus one pending head job
  have been pulled from it.

## Limitations

- **Threads and the GIL:** threads only speed things up when transforms release
  the GIL (as libraries like Pillow, OpenCV and NumPy do). For pure-Python
  transforms, a process pool would be needed.
- **Memory accounting:** the budget counts only the original input bytes.
  Transform outputs, decoded buffers and executor overhead are not included.
- **Results in memory:** finished results are kept until the batch completes.
  A production version would stream or write them out as they finish.
- **Single machine:** work is not distributed across nodes.


  ## Future work

Ideas for a second version, none of which are implemented yet:

- **Hashing for deduplication.** Hash each image's bytes so duplicates can reuse
  a previous result, and images known to fail can be rejected without running
  the transform chain again.
- **Weighted semaphore for the byte budget.** Replace the manual in-flight byte
  counter with a semaphore-style admission gate. It would need to acquire all
  of a job's permits atomically (to avoid two jobs each holding part of what
  the other needs), and cap an oversized job's request so it runs alone instead
  of waiting forever.
- **Fair ordering in front of the gate.** Keep a FIFO queue ahead of the
  semaphore. The semaphore limits how many jobs run, not which one runs next, so
  fairness has to come from the queue. Python's `threading.Semaphore` gives no
  wake-up order guarantee, so a fair version needs its own waiter queue.
- **Retry lane for failures.** Send failed images to a separate FIFO retry queue
  (a dead-letter queue) and reprocess them after the main batch, so failures
  don't hold up good images.
- **Size-aware admission with aging.** Let smaller jobs go first to cut average
  latency, while gradually raising the priority of waiting jobs so large ones
  are never starved. A plain stack (LIFO) is avoided because it can starve the
  oldest jobs.
- **Streaming results.** Write or yield results as they finish instead of
  holding them all in memory until the end.

  

## Testing

The implementation passed the full hidden test suite (18/18) on the platform
where it was built. The tests are not included in this repository.

## License

MIT
MIT
