A self-learning project which follows the book of Eduard Sciore. This implementation differs from the original one with a few aspects:
-  First of all, it is being implemented using the modern Java (22 Currently)
-  The disk operations layer will be replaced by io_uring (A C file library for linux) by panama calls.
-  For the non-heap operations, ByteBuffer API will be replaced by MemorySegment API, which is recently released.

STILL IN DEVELOPMENT.
