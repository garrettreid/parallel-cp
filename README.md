# parallel cp
**`parallel cp`** is a small script for copying a single file by multiple slices in parallel.

Why might you need this, you ask? You probably don't. If you're copying a file that's already on your machine, this will be slower than a regular `cp`. **`parallel cp`** becomes useful when you have a network mounted filesystem with a large file you want to copy, but you are slowed by either a shared connection or high latency.

While it was a fun exercise to build, **`parallel cp`** doesn't do anything that you couldn't already do using [GNU's parallel](https://www.gnu.org/software/parallel/) and `dd`. This script will, however, provide you with a nice progress bar.
