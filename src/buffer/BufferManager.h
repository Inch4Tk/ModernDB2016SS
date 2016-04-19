#pragma once
#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <stdint.h>
#include <utility>

// Forwards
class BufferFrame;

class BufferManager
{
public:
	BufferManager( uint32_t pageCount );
	~BufferManager();

	BufferFrame& FixPage( uint64_t pageId, bool exclusive );
	void UnfixPage( BufferFrame& frame, bool isDirty );

private:

	uint32_t mPageCount;

	void LoadPage( BufferFrame& frame );
	void WritePage( BufferFrame& frame );
	std::pair<uint64_t, uint64_t> SplitPageId( uint64_t pageId );
};

#endif