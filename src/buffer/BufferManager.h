#pragma once
#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <stdint.h>

// Forwards
class BufferFrame;

class BufferManager
{
public:
	BufferManager(uint32_t pageCount);
	~BufferManager();

	BufferFrame& FixPage( uint64_t pageId, bool exclusive );
	void UnfixPage( BufferFrame& frame, bool isDirty );

private:

	uint32_t mPageCount;
};

#endif