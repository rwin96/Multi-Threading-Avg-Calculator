# Java Multithreading: Concurrent Grade Processor


The goal is simple: read a large JSON file containing students and their grades, calculate their average scores concurrently, and save each student's result into a separate JSON file. However, the focus here is on "how" it's done under the hood.

## Project Evolution

There are two main implementations in this repo, showing the progression from a basic thread pool to a more robust pipeline.

### 1. The Basic Approach (`CalculateAverages.java`)
This was my first iteration. It chunks the JSON array and feeds it into a `FixedThreadPool`. The threads calculate the averages and store them in a `ConcurrentHashMap`. Once all threads are done, the main thread writes the results to disk. It works, but it doesn't handle I/O bottlenecks very well.

### 2. The Producer-Consumer Pipeline (`AdvancedCalculatorPipeline.java`)
This is the core of the project. I wanted to optimize the workflow so that calculating (CPU-bound) and writing files (I/O-bound) happen at the exact same time without crashing the system or running out of memory.

Here is what I implemented in this version:
- **Producer-Consumer Pattern:** Separated the workflow. One thread pool is dedicated entirely to math operations, while another handles writing to the disk.
- **Backpressure Handling:** Instead of loading millions of records into memory at once, I used `ArrayBlockingQueue` with fixed capacities. If the disk writers get too slow, the queues fill up and naturally pause the producers. No memory leaks.
- **Graceful Shutdown:** Instead of using volatile boolean flags or forcefully killing threads, I implemented the "Poison Pill" pattern to let threads finish their current batch before shutting down safely.
- **Immutability:** Used Java 17 `record` classes for transferring data between queues to ensure thread safety without needing explicit locks.
