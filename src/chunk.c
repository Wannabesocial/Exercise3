#include <merge.h>
#include <stdio.h>
#include <math.h> //floor(double) function
#include "chunk.h"

#define FIELD_WIDTH 15
#define ID_WIDTH 6

CHUNK *ChunkArray = NULL; //Array of CHUNKS (dynamic)
int chunkArraySize = 0; //size for the Array of CHUNKS

//tururn the numbers of CHUNKS that i have made
int Arraysize(){return chunkArraySize;}

/*we just sets the values in the chunk*/
void SetCHUNK(int file_desc,int from_BlockId,int to_BlockId,int recordsInChunk,int blocksInChunk,CHUNK *chunkModifed){
    
    chunkModifed->file_desc = file_desc;
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

    //Initialize the chunk iterator
    chunk_iterator.chunk_index = 0;
    chunk_iterator.file_desc = fileDesc;

    //Initialize the chunk array (allocate memory) if had alocated in ealryer routine de-allocate first
    chunkArraySize = chunks_to_create;
    if(ChunkArray != NULL)   free(ChunkArray);
    ChunkArray = (CHUNK *)malloc(chunks_to_create * sizeof(CHUNK));
    
    /*initialize every chunk.We go with the chunks-1 becouse all of this are with max posible blocks and records*/
    for(i = 0; i < chunks_to_create-1; i++)
    {
        finishBlockID = startBlockID+blocksInChunk-1;
        SetCHUNK(fileDesc,startBlockID,finishBlockID,HP_GetMaxRecordsInBlock(fileDesc)*blocksInChunk,blocksInChunk,&ChunkArray[i]);
        startBlockID = finishBlockID+1;
    }

    //here we take a specifice care for the last chunk(most propably this has les blocks or records or both)
    blocsInLastChunk = HP_GetIdOfLastBlock(fileDesc) - startBlockID + 1;
    finishBlockID = HP_GetIdOfLastBlock(fileDesc);
    SetCHUNK(fileDesc,startBlockID,finishBlockID,(blocsInLastChunk-1)*HP_GetMaxRecordsInBlock(fileDesc)+HP_GetRecordCounter(fileDesc,finishBlockID),blocsInLastChunk,&ChunkArray[i]);
    
    //return the chunk iterator
    return chunk_iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk){
    
    int CurrentIndex = iterator->chunk_index;
    
    //if the cuurent chunk is the last one
    if(CurrentIndex == chunkArraySize)
    {
        printf("You are in the last CHUNK!\n");
        return 1; //fail
    }

    CHUNK *temp = &ChunkArray[CurrentIndex];
    //we copy paste the data from the spesific array to the CHUNK,so it is not posible to change the CHUNKS,that we return
    SetCHUNK(temp->file_desc,temp->from_BlockId,temp->to_BlockId,temp->recordsInChunk,temp->blocksInChunk,chunk);

    //prepare for the next routine
    iterator->chunk_index++;
    return 0; //succes
}

int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){

    //if the record you want to take is out of this CHUNK
    if(i > chunk->recordsInChunk || i <= 0)
    {
        printf("This record is out of this CHUNK.Here exist only %d records\n",chunk->recordsInChunk);
        return 1; //fail
    }

    //we find the blockID in the CHUNK that have the Record
    int blockID_with_Record = (i-1)/HP_GetMaxRecordsInBlock(chunk->file_desc) + chunk->from_BlockId;

    //we find the spesific record place in the BLOCK
    int Record_Potition = (i-1) - (blockID_with_Record - chunk->from_BlockId) * HP_GetMaxRecordsInBlock(chunk->file_desc);

    //we take the record and Unpin the Block
    HP_GetRecord(chunk->file_desc,blockID_with_Record,Record_Potition,record);
    HP_Unpin(chunk->file_desc,blockID_with_Record);

    return 0;//succes
}

int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record){

    //if the record you want to take is out of this CHUNK
    if(i > chunk->recordsInChunk || i <= 0)
    {
        printf("This record is out of this CHUNK.Here exist only %d records\n",chunk->recordsInChunk);
        return 1; //fail
    }

    //we find the blockID in the CHUNK that have the Record
    int blockID_with_Record = (i-1)/HP_GetMaxRecordsInBlock(chunk->file_desc) + chunk->from_BlockId;

    //we find the spesific record place in the BLOCK
    int Record_Potition = (i-1) - (blockID_with_Record - chunk->from_BlockId) * HP_GetMaxRecordsInBlock(chunk->file_desc);

    //we set the record and Unpin the Block
    HP_UpdateRecord(chunk->file_desc,blockID_with_Record,Record_Potition,record);
    HP_Unpin(chunk->file_desc,blockID_with_Record);
}

void CHUNK_Print(CHUNK chunk){

    //creat a Record Iterator so we can travel throght records
    CHUNK_RecordIterator RecordIterator = CHUNK_CreateRecordIterator(&chunk);
    CHUNK temp = RecordIterator.chunk;
    bool flag;
    Record record;

    //print some data for the current CHUNK
    printf("\nCHUNK detailes:StartID=%d,LastID=%d,TotalBlocks=%d,TotalRecords=%d\n",temp.from_BlockId,temp.to_BlockId,temp.blocksInChunk,temp.recordsInChunk);

    //print nicely the data for the current CHUNK
    printf("+------+---------------+---------------+---------------+\n");
    printf("|%-*s|%-*s|%-*s|%-*s|\n",ID_WIDTH,"ID",FIELD_WIDTH, "NAME", FIELD_WIDTH, "SURNAME", FIELD_WIDTH, "CITY");
    printf("+------+---------------+---------------+---------------+\n");
    for(int i = 0; i < RecordIterator.chunk.recordsInChunk; i++)
    {   
        flag = false;
        CHUNK_GetNextRecord(&RecordIterator,&record);
        printf("|%-*d|%-*s|%-*s|%-*s|\n",ID_WIDTH,record.id,FIELD_WIDTH,record.name,FIELD_WIDTH,record.surname,FIELD_WIDTH,record.city);
        if((i+1) % HP_GetMaxRecordsInBlock(chunk.file_desc) == 0)
        {
            flag = true;
            printf("+------+---------------+---------------+---------------+\n");
        }
    }
    if(flag == false)
        printf("+------+---------------+---------------+---------------+\n");
}

CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk){
    //create a record iterator
    CHUNK_RecordIterator RecordIterator;

    //initialize the record iterator
    SetCHUNK(chunk->file_desc,chunk->from_BlockId,chunk->to_BlockId,chunk->recordsInChunk,chunk->blocksInChunk,&RecordIterator.chunk);
    RecordIterator.currentBlockId = chunk->from_BlockId;
    RecordIterator.cursor = 0;
    
    //return the record iterator
    return RecordIterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator,Record* record){
    
    //if you had in the last call the last record in the last block then error mesege and return 1
    if(iterator->currentBlockId > iterator->chunk.to_BlockId)
    {
        printf("You are uot of records in this CHUNK!\n");
        return 1; //fail
    }

    //copy paste the curent record
    HP_GetRecord(iterator->chunk.file_desc,iterator->currentBlockId,iterator->cursor,record);

    //prepare for the next routine
    iterator->cursor++;

    //if we were the last record in this block we take the next one(block) (== couse the cursor=0 is the 1st record)
    if(iterator->cursor == HP_GetRecordCounter(iterator->chunk.file_desc,iterator->currentBlockId))
    {
        //unpin the block that we used before we take the next one
        HP_Unpin(iterator->chunk.file_desc,iterator->currentBlockId);

        iterator->currentBlockId++;
        iterator->cursor = 0; //we start again in the 1st record
    }
    return 0; //succes
}
