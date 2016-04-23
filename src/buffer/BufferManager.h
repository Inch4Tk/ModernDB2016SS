#pragma once
#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include "BufferFrame.h"
#include "utility/RWLock.h"

#include <stdint.h>
#include <utility>
#include <vector>
#include <unordered_map>
#include <mutex>

/// <summary>
/// Concurrent Buffer Manager, enabling loading from and flushing to disk.
/// </summary>
class BufferManager
{
public:
	BufferManager( uint32_t pageCount );
	~BufferManager();

	BufferFrame& FixPage( uint64_t pageId, bool exclusive );
	void UnfixPage( BufferFrame& frame, bool isDirty );

private:
	uint32_t mPageCount;
	// Memory and Buffer related
	uint8_t* mBufferMemory = nullptr;
	std::vector<BufferFrame> mFrames;
	RWLock mHashMapLock;
	std::unordered_map<uint64_t, BufferFrame*> mLoadedFrames;
	std::vector<std::mutex*> mFileIOLocks; // These are used to prevent parallel write/read to the same page on disk

	// Stats
	std::atomic<uint64_t> mNotRequestedPages; // Number of times we received a not requested page
	std::atomic<uint64_t> mPageMisses; // Number of times our page was not in the map when we requested
	std::atomic<uint64_t> mDirtyWritebacks; // Number of times we wrote back dirty pages during operation
	std::atomic<uint64_t> mPageReplacementRetries; // Number of times we had to retry on page replacement
	std::atomic<uint64_t> mSimulPageLoadTries; // Number of times somebody else loaded a page we were just requesting

	// Helpers
	BufferFrame* FixPageReplacement( uint64_t pageId, bool exclusive );
	BufferFrame* FindReplacementPage();
	void LoadPage( BufferFrame& frame );
	void WritePage( BufferFrame& frame );
	inline BufferFrame* CheckSamePage( uint64_t pageId, bool exclusive, BufferFrame* frame );
	std::pair<uint64_t, uint64_t> SplitPageId( uint64_t pageId );
};

#endif