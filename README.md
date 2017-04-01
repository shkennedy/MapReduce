Shane Kennedy

MapReduce csv Reader

Includes 5 forms of MapReduce
1) 1 map thread per file, each writes analysis to given struct
2) N map threads, writes to given struct
3) N map threads, writes to shared file, reduce thread given priority to file
4) N map threads, writes to global buffer, writers given priority
5) N map threads, writes to socket connected to reduce thread
