#include "SPSegment.h"

#include "utility/helpers.h"
#include "BufferManager.h"
#include "SlottedPage.h"
#include "DBCore.h"

/// <summary>
/// Initializes a new instance of the <see cref="SPSegment"/> class.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <param name="pagecount">The pagecount.</param>
SPSegment::SPSegment( DBCore& core, BufferManager& bm, uint64_t segmentId ) :
	mCore(core), mBufferManager(bm), mSegmentId(segmentId)
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
	std::pair<uint64_t, uint64_t> pIdsId = SplitTID( tid );
	BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, pIdsId.first ), false );
	SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());
	
	// Checks if tid is valid
	if ( !page->IsInitialized() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Record( 0, nullptr );
	}
	SlottedPage::Slot* slot = page->GetSlot( pIdsId.second );
	if ( !slot )
	{
		mBufferManager.UnfixPage( frame, false );
		return Record( 0, nullptr );
	}
	if ( slot->IsFree() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Record( 0, nullptr );
	}
	// We found a page and a non-empty slot. Check the options in our slot
	if ( slot->IsOtherRecordTID() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Lookup( slot->GetOtherRecordTID() ); // Recursive call to other page
	}
	uint32_t offset = slot->GetOffset();
	uint32_t length = slot->GetLength();
	if ( slot->IsFromOtherPage() )
	{
		offset += 8; // Currently we do nothing with the original tid, but it is there
	}
	Record r = Record( length, reinterpret_cast<uint8_t*>(frame.GetData()) + offset );
	mBufferManager.UnfixPage( frame, false );
	return r;
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
