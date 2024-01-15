#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include <math.h> // floor(double) function
#include "chunk.h"
#include "record.h"
#include "bf.h"
#include "hp_file.h"
#include "sort.h"

#define NOT_FOUND -1
#define OUT_OF_RECORDS -2
#define YES -3

//checks if a given array of records is empty (all records have ID set to NOT_FOUND).
bool IsEmpty(Record *record, int size) {
    for (int i = 0; i < size; i++)
        if (record[i].id != NOT_FOUND)
            return false;
    return true;
}

//finds the smallest record in the array of records and marks it as processed.
Record findSmallestRecord(Record *records, int numRecords) {
    Record smallestRecord;
    int smallestRecord_position;

    //find the first non-processed record as the initial smallest record
    for (int i = 0; i < numRecords; i++) {
        if (records[i].id != NOT_FOUND) {
            smallestRecord = records[i];
            smallestRecord_position = i;
            break;
        }
    }

    //iterate through the records to find the smallest one
    for (int j = 0; j < numRecords; j++) {
        if (records[j].id == NOT_FOUND)
            continue;

        //use the shouldSwap function to compare records
        if (shouldSwap(&smallestRecord, &records[j])) {
            smallestRecord = records[j];
            smallestRecord_position = j;
        }
    }

    //mark the smallest record as processed by setting its ID to NOT_FOUND
    records[smallestRecord_position].id = NOT_FOUND;

    return smallestRecord;
}

//merges chunks from a file using an external merge sort algorithm with b-way merging.
void merge(int file_desc, int chunkSize, int bWay, int output_fd) {

    //mreate a chunk iterator to iterate through chunks
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, chunkSize);
    CHUNK inputChunk;

    //find out how many loops are needed to merge using at most bWay chunks per iteration
    double estimate_loops = Arraysize() / (double) bWay;
    int loops_to_merge = (int) (floor(estimate_loops) == estimate_loops) ? estimate_loops : estimate_loops + 1;
    int size = bWay;

    //create arrays for record iterators, records, and flags to track records in chunks
    CHUNK_RecordIterator record_iterator[size];
    Record records[size];
    int Records_In_Chunks[size];

    //start the merge algorithm based on the calculated number of loops
    for (int i = 0; i < loops_to_merge; i++) {

        //in the last loop, adjust the size for the remaining chunks
        if (i == loops_to_merge - 1) {
            size = Arraysize() - (bWay * i);
        }

        //initialize arrays and iterators for each chunk
        for (int j = 0; j < size; j++) {
            //initialize the array with records
            records[j].id = NOT_FOUND;

            //initialize the iterators
            CHUNK_GetNext(&iterator, &inputChunk);
            record_iterator[j] = CHUNK_CreateRecordIterator(&inputChunk);

            Records_In_Chunks[j] = YES;
        }

        //continue until all records are processed
        while (true) {
            //iterate through the array for records without an actual record (take the next one)
            for (int i = 0; i < size; i++) {
                if (records[i].id == NOT_FOUND && Records_In_Chunks[i] == YES) {
                    if (CHUNK_GetNextRecord(&record_iterator[i], &records[i]) == 1) {
                        records[i].id == NOT_FOUND;
                        Records_In_Chunks[i] = OUT_OF_RECORDS;
                    }
                }
            }

            //if all records are stored in the new file, exit the loop
            if (IsEmpty(records, size))
                break;

            //the array of records is not empty, so insert the lowest record into the new file
            HP_InsertEntry(output_fd, findSmallestRecord(records, size));
        }
    }
}