#include "SPSegment.h"

#include "BufferManager.h"

/// <summary>
/// Initializes a new instance of the <see cref="SPSegment"/> class.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <param name="pagecount">The pagecount.</param>
SPSegment::SPSegment( BufferManager& bm, uint64_t segmentId, uint64_t pagecount ) : 
	mBufferManager(bm), mSegmentId(segmentId), mPagecount(pagecount)
{
}

/// <summary>
/// Finalizes an instance of the <see cref="SPSegment"/> class.
/// </summary>
SPSegment::~SPSegment()
{
}

/// <summary>
/// Searches through the segment's pages looking for a page with enough space to store r. 
/// Returns the TID identifying the location where r was stored.
/// </summary>
/// <param name="r">The r.</param>
/// <returns></returns>
TID SPSegment::Insert( const Record& r )
{
	//TODO
	return 0;
}

/// <summary>
/// Removes the record specified by tid. Updates the page header accordingly.
/// </summary>
/// <param name="tid">The tid.</param>
/// <returns></returns>
bool SPSegment::Remove( TID tid )
{
	//TODO
	return true;
}

/// <summary>
/// Retrieves the record specified by tid. Returns a read-only copy.
/// </summary>
/// <param name="tid">The tid.</param>
/// <returns></returns>
Record SPSegment::Lookup( TID tid )
{
	//TODO
	return Record(0, nullptr);
}

/// <summary>
/// Updates the content of record specified by tid with content of record r.
/// </summary>
/// <param name="tid">The tid.</param>
/// <param name="r">The r.</param>
/// <returns></returns>
bool SPSegment::Update( TID tid, const Record& r )
{
	//TODO
	return true;
}
