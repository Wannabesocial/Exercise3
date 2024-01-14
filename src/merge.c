#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include "chunk.h"
#include "record.h"
#include "bf.h"
#include "hp_file.h"
#define INT_MAX 2147483647

bool MshouldSwap(Record* rec1, Record* rec2) {
    return rec1->id > rec2->id; // Swap if id of rec1 is greater than id of rec2
}

void merge(int file_desc, int chunkSize, int bWay, int output_fd) {
    // Initialize iterators and variables
    CHUNK_Iterator* chunkIterators = malloc(bWay * sizeof(CHUNK_Iterator));
    CHUNK* chunks = malloc(bWay * sizeof(CHUNK));
    Record* records = malloc(bWay * sizeof(Record));
    int* recordIndices = malloc(bWay * sizeof(int));
    int minRecordIndex, minChunkIndex, totalRecordsMerged = 0;

    // Create iterators for chunks
    for (int i = 0; i < bWay; i++) {
        chunkIterators[i] = CHUNK_CreateIterator(file_desc, chunkSize);
        CHUNK_GetNext(&chunkIterators[i], &chunks[i]);
        recordIndices[i] = 0;
    }

    // Merge chunks
    while (totalRecordsMerged < bWay * chunkSize) {
        // Load next records from each chunk
        for (int i = 0; i < bWay; i++) {
            if (recordIndices[i] < chunks[i].recordsInChunk) {
                CHUNK_GetIthRecordInChunk(&chunks[i], recordIndices[i] + 1, &records[i]);
            } else {
                records[i].id = INT_MAX; // Mark as invalid record
            }
        }

        // Find the minimum record among all chunks
        minRecordIndex = -1;
        for (int i = 0; i < bWay; i++) {
            if (records[i].id != INT_MAX && (minRecordIndex == -1 || MshouldSwap(&records[i], &records[minRecordIndex]))) {
                minRecordIndex = i;
            }
        }

        // Write the minimum record to the output file
        if (minRecordIndex != -1) {
            HP_InsertEntry(output_fd, records[minRecordIndex]);
            totalRecordsMerged++;
            recordIndices[minRecordIndex]++;
        } else {
            break; // No valid record found to write
        }
    }

    // Close all iterators
    for (int i = 0; i < bWay; i++) {
        // Here you may need to implement additional logic to unpin blocks and handle iterators properly
        HP_Unpin(file_desc, chunks[i].from_BlockId); // Unpin the block associated with the chunk
    }

    free(chunkIterators);
    free(chunks);
    free(records);
    free(recordIndices);
}


