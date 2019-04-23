# Task1AMQPSistemesDistribuits

Implementation of MapReduce architecture


The idea of the project is the implementation of MapReduce architecture that it enable the parallel processing of big data.

First you have huge amounts of data and it is broken into smaller datasets to be processed separately on different worker nodes (in this project are wordcounts) whose results are merged into only one result.
This architecture have different steps:

1. The program receive amounts of data.
2. It splits data in different parts.
3. These parts are processed in parallel (this work is performed for wordcount workers).
4. Finally partial results are merged in only one result that is shown to final user.
In the real MapReduce Architecture we have shuffing step but we implemented a simplified MapReduce Architecture and this step is not implemented.
To show the performance of this architecture, we implemented the processing of the words in a text file. The idea is generate a final dictionary with all words that contains the original text file and count all words. For example, if we have example_file.txt:

Hello Hello GoodBye Hello

The solution should be:

- Wordcount: {“Hello”: 3, “GoodBye”: 1}.
- CountWords: 4.


In this solution we have performed the steps of this architecture with:

* Wordcount: It represents the parallel processing of smalled datasets.
* Reducer: It represents the merge of the different partial solutions done by wordcount workers.
* CountWords: It only count the total words in the final result and show it.

## FOR MORE DETAILS READ TASK1 DOCUMENTATION PDF ###
