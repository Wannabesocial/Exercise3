#include <merge.h>
#include <stdio.h>
#include <math.h> //floor(double) function
#include "chunk.h"

/*we just sets the values in the chunk*/
void SetCHUNK(int from_BlockId,int to_BlockId,int recordsInChunk,int blocksInChunk,CHUNK *chunkModifed)
{
    chunkModifed->from_BlockId = from_BlockId;
    chunkModifed->to_BlockId = to_BlockId;
    chunkModifed->blocksInChunk = blocksInChunk;
    chunkModifed->recordsInChunk = recordsInChunk;
}

CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk){
    //create a chunk iterator
    CHUNK_Iterator chunk_iterator;
    CHUNK *chunkModifed;

    double estimate_chunks = HP_GetIdOfLastBlock(fileDesc)/(double)blocksInChunk;
    //if chunks is float with .0 then is fine else we do it +1 (11.2 = 12, 15.0 = 15)
    int chunks_to_create = (int)(floor(estimate_chunks) == estimate_chunks) ? estimate_chunks : estimate_chunks + 1;
    int startBlockID = 1,finishBlockID = 0,blocsInLastChunk,i;

    //alocate memory for chunks and initialize the chunk iterator
    chunk_iterator.chunk_index = 0;
    chunk_iterator.chunkSize = chunks_to_create;
    chunk_iterator.file_desc = fileDesc;
    chunk_iterator.chunk = (CHUNK *)malloc(chunks_to_create * sizeof(CHUNK));
    
    /*initialize every chunk in chunk iterator.We go with the chunks-1 becouse all of this are with max posible blocks and records*/
    for(i = 0; i < chunks_to_create-1; i++)
    {
        finishBlockID = startBlockID+blocksInChunk-1;
        SetCHUNK(startBlockID,finishBlockID,HP_GetMaxRecordsInBlock(fileDesc)*blocksInChunk,blocksInChunk,&chunk_iterator.chunk[i]);
        startBlockID = finishBlockID+1;
    }

    //here we take a specifice care for the last chunk(most propably this has les blocks or records or both)
    blocsInLastChunk = HP_GetIdOfLastBlock(fileDesc) - startBlockID + 1;
    finishBlockID = HP_GetIdOfLastBlock(fileDesc);
    SetCHUNK(startBlockID,finishBlockID,(blocsInLastChunk-1)*HP_GetMaxRecordsInBlock(fileDesc)+HP_GetRecordCounter(fileDesc,finishBlockID),blocsInLastChunk,&chunk_iterator.chunk[i]);
    
    //return the chunk iterator
    return chunk_iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk){
    
    int CurrentIndex = iterator->chunk_index;
    
    //if the cuurent chunk is the last one
    if(CurrentIndex == iterator->chunkSize)
    {
        printf("You are in the last CHUNK!\n");
        return 1;
    }

    CHUNK *temp = &iterator->chunk[CurrentIndex];
    //we copy paste the data from the spesific array to the CHUNK that we return
    SetCHUNK(temp->from_BlockId,temp->to_BlockId,temp->recordsInChunk,temp->blocksInChunk,chunk);

    //for the next routine
    iterator->chunk_index++;
    return 0;
}

int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){

}

int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record){

}

void CHUNK_Print(CHUNK chunk){

}


CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk){

}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator,Record* record){
    
}
