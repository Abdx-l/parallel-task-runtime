# Parallel Task Runtime

C++17 work-stealing task runtime with bounded Chase-Lev owner deques, an external submission queue, futures, cooperative work distribution, parallel_for, and parallel merge sort. Task allocations remain owned until runtime destruction to keep stolen pointers valid.

## Run

`cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure`. Tests compare one million sorted integers with std::sort. Deques have fixed capacity 65536; overflow falls back to the injection queue.
