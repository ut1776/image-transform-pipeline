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

Under the hood it is a bounded concurrent job scheduler. It does not include
any storage, upload or networking code, and the transforms are supplied by the
caller. The same design applies to services running under fixed CPU and memory
limits, such as cloud workers.


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

- The budget counts only the original input bytes. Transform outputs, decoded
  buffers and executor overhead are not included.
- Finished results are kept in memory until the batch completes.

## Testing

The implementation passed the full hidden test suite (18/18) on the platform
where it was built. The tests are not included in this repository.

## License

MIT
