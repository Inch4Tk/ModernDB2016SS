#pragma once
#ifndef SPSEGMENT_H
#define SPSEGMENT_H

#include "relation/Record.h"
#include "utility/defines.h"

#include <stdint.h>

// Forwards
class BufferManager;

/// <summary>
/// Segment that operates on slotted pages.
/// </summary>
class SPSegment
{
public:
	SPSegment( BufferManager& bm, uint64_t segmentId, uint64_t pagecount );
	~SPSegment();
	
	// Record management
	TID SPSegment::Insert( const Record& r );
	bool SPSegment::Remove( TID tid );
	Record SPSegment::Lookup( TID tid );
	bool SPSegment::Update( TID tid, const Record& r );

private:
	BufferManager& mBufferManager;
	uint64_t mSegmentId;
	uint64_t mPagecount;
};

#endif