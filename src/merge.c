#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include <math.h> //floor(double) function
#include "chunk.h"
#include "record.h"
#include "bf.h"
#include "hp_file.h"
#include "sort.h"

#define NOT_FOUND -1
#define OUT_OF_RECORDS -2
#define YES -3

bool IsEmpty(Record *record,int size)
{
    for(int i = 0; i < size; i++)
        if(record[i].id != NOT_FOUND)
            return false;
    return true;
}

Record findSmallestRecord(Record *records, int numRecords)
{
    Record smallestRecord;
    int smallestRecord_position;

    for(int i = 0; i < numRecords; i++)
    {
        if(records[i].id != NOT_FOUND)
        {
            smallestRecord = records[i];
            smallestRecord_position = i;
            break;
        }
    }
    
    for (int j = 0; j < numRecords; j++)
    {   
        if(records[j].id == NOT_FOUND)
            continue;

        if (shouldSwap(&smallestRecord, &records[j]))
        {
            smallestRecord = records[j];
            smallestRecord_position = j;
        }
    }

    records[smallestRecord_position].id = NOT_FOUND;

    return smallestRecord;
}


void merge(int file_desc, int chunkSize, int bWay, int output_fd) {
    

    //we create a chunk iterator so we make the chunks with the data
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc,chunkSize);
    CHUNK inputChunk;

    //find out how many loops we must do to merge using at most bWay chunks per time
    double estimate_loops =  Arraysize() / (double)bWay;
    int loops_to_merge = (int)(floor(estimate_loops) == estimate_loops) ? estimate_loops : estimate_loops + 1;
    int size = bWay;

    //we use a array of record itaratos (one for every chunk)
    CHUNK_RecordIterator record_iterator[size];
    Record records[size];
    int Records_In_Chunks[size];

    //here we start the merge algorith based on the loops we calculate earlyer
    for(int i = 0; i < loops_to_merge; i++)
    {
        //last loop
        if(i == loops_to_merge-1)
        {
            //we must store the right size for the leftovers
            size = Arraysize() - (bWay * i);
        }
        
        for(int j = 0; j < size; j++)
        {
            //init the array with the records
            records[j].id = NOT_FOUND;

            //init the itereators
            CHUNK_GetNext(&iterator,&inputChunk);
            record_iterator[j] = CHUNK_CreateRecordIterator(&inputChunk);

            Records_In_Chunks[j] = YES;
        }

        while(true)
        {
            //in array for every record that has no actuall record (take the next one)
            for(int i = 0; i < size; i++)
            {
                if(records[i].id == NOT_FOUND && Records_In_Chunks[i] == YES)
                {
                    if(CHUNK_GetNextRecord(&record_iterator[i],&records[i]) == 1)
                    {
                        records[i].id == NOT_FOUND;
                        Records_In_Chunks[i] = OUT_OF_RECORDS;
                    }
                }
                
            }
            
            //if we store all the records in the new file
            if(IsEmpty(records,size))
                break;

            //the array of records is not empty so we can go and insert in the new file the lowest record
            HP_InsertEntry(output_fd,findSmallestRecord(records,size));
        }
    }
}


