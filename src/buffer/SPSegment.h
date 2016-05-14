#pragma once
#ifndef SPSEGMENT_H
#define SPSEGMENT_H

#include "relation/Record.h"
#include "utility/defines.h"

#include <stdint.h>

// Forwards
class DBCore;
class BufferManager;

/// <summary>
/// Segment that operates on slotted pages.
/// </summary>
class SPSegment
{
public:
	SPSegment( DBCore& core, BufferManager& bm, uint64_t segmentId );
	~SPSegment();
	
	// Record management
	TID Insert( const Record& r );
	bool Remove( TID tid );
	Record Lookup( TID tid );
	bool Update( TID tid, const Record& r );

private:
	DBCore& mCore;
	BufferManager& mBufferManager;
	uint64_t mSegmentId;

	uint64_t FindFreePage( uint32_t minSpace );
};

#endif