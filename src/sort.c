#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

bool shouldSwap(Record* rec1, Record* rec2) {
    int nameComparison = strcmp(rec1->name, rec2->name);//strcmp returns 0 if strings are equal
    if (nameComparison == 0) {//if names are the same compare surnames
        return strcmp(rec1->surname, rec2->surname) > 0;
    } else {
        return nameComparison > 0;
    }
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk) {
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);//create iterator for chunks
    CHUNK chunk;

    // Get the total number of chunks
    int totalChunks = Arraysize();

    // Iterate through chunks
    for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
        // Get the current chunk
        CHUNK_GetNext(&iterator, &chunk);

        // Sort the current chunk
        sort_Chunk(&chunk);

        // If it's the last chunk, break from the loop
        if (chunkIndex == totalChunks - 1) {
            break;
        }
    }
}
void sort_Chunk(CHUNK* chunk) {
    int i, j;
    Record temp;

    for (i = 0; i < chunk->recordsInChunk - 1; i++) {//bubble sort
        for (j = 0; j < chunk->recordsInChunk - i - 1; j++) {
            Record record1, record2;//get records
            CHUNK_GetIthRecordInChunk(chunk, j + 1, &record1);//get record in position j+1
            CHUNK_GetIthRecordInChunk(chunk, j + 2, &record2);//get record in position j+2

            if (shouldSwap(&record1, &record2)) {//if we should swap them
                CHUNK_UpdateIthRecord(chunk, j + 1, record2);
                CHUNK_UpdateIthRecord(chunk, j + 2, record1);
            }
        }
    }
}

