/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Authors: duanshuai 
 * Date: 05/15/2021 
 * Filename: buffer.cpp
 * Purpose of File: Defines functions described in buffer.h
 * This is where the majority of our buffer manager is defined.
 * This file contains functions for constructing a buffMgr, destructing a buffMgr,
 * allocating a buffer frame using the clock algorithm, flushing dirty files and
 * other page operations
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {


BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++)
    {
  	    bufDescTable[i].frameNo = i;
  		bufDescTable[i].valid = false;
    }

  	bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  	clockHand = bufs - 1;
}

//更新脏页 
BufMgr::~BufMgr() {
	//若对应页框为dirty， 
    for(unsigned int i = 0; i < numBufs; i++){
        if(bufDescTable[i].dirty == true){
            flushFile(bufDescTable[i].file);
        }
    }
    delete [] bufDescTable;
    delete [] bufPool;
    delete hashTable;
}

//clockhand前进 
void BufMgr::advanceClock()
{
    clockHand = (clockHand+1) % numBufs;
}


//分配页框 
void BufMgr::allocBuf(FrameId & frame)
{
	/*
    思路:
    从valid = 1, pin = 0且refbit = 0处替换 
        (1)dirty=1(写回后替换) || dirty=0（替换） 
	
	step： advanceclock，不断找合适frame
	       若全被pinned，异常
	       if(valid)
		        if(refbit)
			        if(pin)
		    
    */
    //指示是否找到合适页框 
    bool findNext = false;
    unsigned int pinned = 0;
    
    while(!findNext){
        advanceClock();
        //若所有页框都被pin
        if(pinned == numBufs){
            throw BufferExceededException();
        }
        /*
        有效：继续判断pin与refbit 
		无效：使用invalid位进行替换 
		*/ 
        if (bufDescTable[clockHand].valid == false){
            bufDescTable[clockHand].Clear();
            frame = clockHand;
            findNext = true;
        }
        /*
        refbit = 1:将划过的refbit置0
		refbit = 0:判断dirty位
		           if dirty = 1:写回
				   if dirty = 0:替换 
		*/ 
        else if(bufDescTable[clockHand].refbit == true){
            bufDescTable[clockHand].refbit = false;
        }
        /*
        pinCnt > 0:禁止分配，pinnednum++
		pinCnt = 0:判断dirty位
		           if dirty = 1:写回
				   if dirty = 0:替换   
		*/ 
        else if(bufDescTable[clockHand].pinCnt > 0){
            pinned++;
        }

        else if(bufDescTable[clockHand].refbit == false){
            if(bufDescTable[clockHand].dirty == true){
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            }
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            bufDescTable[clockHand].Clear();
            frame = clockHand;
            findNext = true;
        }
    }
}

//将文件从磁盘读入缓冲区 
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frame;
    try{
        hashTable->lookup(file, pageNo, frame);
        //case1：文件在缓冲池 
        bufDescTable[frame].refbit = true;
        bufDescTable[frame].pinCnt++;
        page = &bufPool[frame];

    }catch(HashNotFoundException e){
    	//case2：文件不在缓冲池 
        FrameId frame;
        allocBuf(frame);
        //printf("This is frame#: %id\n", frame);
        bufPool[frame] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
        bufDescTable[frame].Set(file, pageNo);
        page = &bufPool[frame];
    }
}

//释放引用 
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  FrameId frame;
    try{
        hashTable->lookup(file, pageNo, frame);
        if(bufDescTable[frame].pinCnt > 0){
            bufDescTable[frame].pinCnt--;
            if (dirty == true){
                bufDescTable[frame].dirty = dirty;
            }
        }
        else{
            throw PageNotPinnedException("Ping is already 0", pageNo, frame);
        }
    }catch(HashNotFoundException e){

    }
}


//将缓冲池内容更新到file，清除缓冲池 
void BufMgr::flushFile(const File* file)
{
    for(unsigned int i = 0; i < numBufs; i++){
        if(bufDescTable[i].file == file){
            if(bufDescTable[i].pinCnt != 0){
                throw PagePinnedException("This removing page is already being used", bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }
            if(bufDescTable[i].valid == false){
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, false, bufDescTable[i].refbit);
            }
            if(bufDescTable[i].dirty == true){
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            hashTable->remove(file, bufPool[i].page_number());
            bufDescTable[i].Clear();
        }
    }
}

//获取文件的页号和页信息 
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
    FrameId frame;
    Page new_page = file->allocatePage();
    allocBuf(frame);
    //更新hashtable 
    hashTable->insert(file,new_page.page_number(), frame);
    //更新页框信息 
	bufDescTable[frame].Set(file, new_page.page_number());
    //获得page信息和frame信息 
	pageNo = new_page.page_number();
    bufPool[frame] = new_page; 
    page = &bufPool[frame];
}

//删除文件中的页 
/*
若页已经在缓冲池中分配frame，则要同时释放相应frame 
*/ 
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frame;
    try{
        hashTable->lookup(file, PageNo, frame);
        bufDescTable[frame].Clear();
        file->deletePage(PageNo);
        hashTable->remove(file, PageNo);
    }catch(HashNotFoundException e){
        file->deletePage(PageNo);
    }
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
