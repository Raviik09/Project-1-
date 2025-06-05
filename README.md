# Lab 1: OLC_BTree

## Codebase structure

- OLC_BTree skeleton code provided in `include/OLC_Btree.hpp`
- Optimistic Lock Coupling primitives are provided in `include/OptLatch.hpp` 
- Task implementation in `src/OLC_BTree_Stencil.cpp`

## Testing

- basic tests are found in `test/basic.cpp` and cover basic upsert and lookup operations
- advanced tests are ran by the server runner and will perform operations on your tree with multiple threads simultaneously and with much larger data-sizes. You should improve the basic tests to replicate the advanced test behavior and test locally before submitting to the runner.

- You can compile the project on the development VM with:
 ```
 mkdir build
 cd build
 cmake ..
 make
 ./olc_btree_test_basic
 ```

## Your task

- Implement a B+ Tree which can support upsert (update+insert) and lookup operations while being resilient to potential race conditions from multi-threading & achieve good multi-core scalability.
- You should use the OLC primitives provided to introduce the locking mechanisms into your tree implementation.
- Use the paper, B+ tree slides as well as the Lab1 Overview slides to help you.
- Use of LLMs such as ChatGPT and code-sharing between students is not permitted and will be counted as a cheating attempt. Write your own code, you are here to learn how things actually work.

## Grading

- 50% of the points are granted for passing all basic tests, another 50% for passing all advanced tests. You gain an additional 3 points if you outperform the baseline implementation, which will be the user named 'Baseline' on the leaderboard.
- We are going to check your code to make sure you did infact implement an OLC_BTree.

## Other notes

- We further discuss the papers, codebase as well as theoretical aspects of OLC and Btrees in the live exercise sessions. Please join them if you have questions!
