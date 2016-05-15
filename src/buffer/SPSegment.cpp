#include "SPSegment.h"

#include "utility/helpers.h"
#include "BufferManager.h"
#include "SlottedPage.h"
#include "DBCore.h"

#include <cassert>

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
	// Loop until we find a free page
	while (true)
	{
		uint64_t pageId = FindFreePage( r.GetLen() );
		BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, pageId ), true );
		SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());
		// We could have an uninitialized page, we have to initialize that page first.
		// Fresh pages will all pass the second if test, else we would have thrown in FindFreePage
		if ( !page->IsInitialized() )
		{
			page->Initialize();
			// TODO: tell schema to add another page to relation
		}
		if ( page->GetFreeContSpace() >= r.GetLen() )
		{
			// Everything worked we do our insert, release page and return our tid
			TID newTID = MergeTID( pageId, page->GetFirstFreeSlotId() );
			uint32_t insertDataBegin = page->GetDataStart() - r.GetLen();

			// Find a slot and update slot
			SlottedPage::Slot* slot = page->GetFirstFreeSlot();
			slot->SetInPage();
			slot->SetOffset( insertDataBegin );
			slot->SetLength( r.GetLen() );
			
			// Update page header
			page->UsedFirstFreeSlot();
			page->SetDataStart( insertDataBegin );

			mBufferManager.UnfixPage( frame, true );
			return newTID;
		}
		else
		{
			mBufferManager.UnfixPage( frame, false );
		}
	}
	assert( false );
	return 0;
}

/// <summary>
/// Removes the record specified by tid. Updates the page header accordingly.
/// Will return false, if no record was found for this TID.
/// </summary>
/// <param name="tid">The tid.</param>
/// <returns></returns>
bool SPSegment::Remove( TID tid )
{
	std::pair<uint64_t, uint64_t> pIdsId = SplitTID( tid );
	BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, pIdsId.first ), true );
	SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());

	// Checks if tid is valid
	if ( !page->IsInitialized() )
	{
		mBufferManager.UnfixPage( frame, false );
		return false;
	}
	SlottedPage::Slot* slot = page->GetSlot( pIdsId.second );
	if ( !slot || slot->IsFree() )
	{
		mBufferManager.UnfixPage( frame, false );
		return false;
	}
	// We found a page and a non-empty slot. Check the options in our slot
	if ( slot->IsOtherRecordTID() )
	{
		// Clean up this old slot and then call remove for other tid
		TID newTid = slot->GetOtherRecordTID();
		page->FreeSlot( pIdsId.second );
		mBufferManager.UnfixPage( frame, true );
		return Remove( newTid );
	}
	// Record is not on another page
	uint32_t offset = slot->GetOffset();
	uint32_t length = slot->GetLength();
	if ( slot->IsFromOtherPage() )
	{
		length += 8; // Currently we do nothing with the original tid, but it is there, so we also want to delete it.
	}
	page->FreeSlot( pIdsId.second );
	page->FreeData( offset, length );
	mBufferManager.UnfixPage( frame, true );
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
	if ( !slot || slot->IsFree() )
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

/// <summary>
/// Finds a page with at least minSpace free continuous memory.
/// </summary>
/// <param name="minSpace">The minimum space.</param>
/// <returns></returns>
uint64_t SPSegment::FindFreePage( uint32_t minSpace )
{
	// Check if the space is bigger than Pagesize - (header + 1 slot)
	// If that is the case we throw, since we currently don't support records bigger than a single page.
	if ( minSpace > DB_PAGE_SIZE - 24 )
	{
		throw std::runtime_error( "Error: Don't support records bigger than a single page." );
	}
	// Linear search over pages
	bool found = false;
	uint64_t curPage = 0;
	while ( !found )
	{
		BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, curPage ), false );
		SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());
		uint32_t contSpace = page->GetFreeContSpace();
		mBufferManager.UnfixPage( frame, false );
		if ( !page->IsInitialized() || contSpace > minSpace )
		{
			// Page is either a fresh page, which of course means we dont yet have any values set
			// this also means free space is not correct yet. But record has to fit, since
			// else it would throw above.
			return curPage;
		}
		++curPage;
	}
	assert( false );
	return 0;
}
