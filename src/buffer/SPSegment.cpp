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
	return Insert( r, false );
}

/// <summary>
/// Inserts the specified r. Sets the slot to from other page if bool is true. (Assumes 8 byte backlink tid is prepended to record)
/// </summary>
/// <param name="r">The r.</param>
/// <param name="setFromOtherPage">if set to <c>true</c> [set from other page].</param>
/// <returns></returns>
TID SPSegment::Insert( const Record& r, bool setFromOtherPage )
{
	// Loop until we find a free page
	while ( true )
	{
		uint64_t pageId = FindFreePage( r.GetLen() );
		BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, pageId ), true );
		SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());
		// We could have an uninitialized page, we have to initialize that page first.
		// Fresh pages will all pass the second if test, else we would have thrown in FindFreePage
		if ( !page->IsInitialized() )
		{
			page->Initialize();
			mCore.AddPagesToRelation( mSegmentId, 1 );
		}
		if ( page->GetFreeContSpace() >= r.GetLen() + 8 )
		{
			// Everything worked we do our insert, release page and return our tid
			TID newTID = MergeTID( pageId, page->GetFirstFreeSlotId() );
			assert( page->GetDataStart() > page->GetFreeContSpace() );
			assert( page->GetDataStart() >= r.GetLen() + 16 + 8 * (page->GetSlotCount() + 1) );
			uint32_t insertDataBegin = page->GetDataStart() - r.GetLen();

			// Find a slot and update slot
			SlottedPage::Slot* slot = page->GetFirstFreeSlot();
			if (setFromOtherPage)
			{
				slot->SetFromOtherPage();
			}
			else
			{
				slot->SetInPage();
			}
			slot->SetOffset( insertDataBegin );
			slot->SetLength( r.GetLen() );

			// Update page header
			page->UsedFirstFreeSlot();
			page->SetDataStart( insertDataBegin );
			memcpy( page->GetDataPointer( insertDataBegin ), r.GetData(), r.GetLen() );

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
		// We assume backlink tid is always the one known to the outside
		// Therefore this one is already removed and we are inside a recursive call
		// So we just add it to the length and remove the whole thing
		length += 8;
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
		offset += 8; // Skip Backlink TID
		length -= 8;
	}
	Record r = Record( length, reinterpret_cast<uint8_t*>(frame.GetData()) + offset );
	mBufferManager.UnfixPage( frame, false );
	return r;
}

/// <summary>
/// Updates the content of record specified by tid with content of record r.
/// Will return false if tid is invalid.
/// </summary>
/// <param name="tid">The tid.</param>
/// <param name="r">The r.</param>
/// <returns></returns>
bool SPSegment::Update( TID tid, const Record& r )
{
	// If the new record is smaller or equal to the old record we just reuse the current record slot
	// If it is bigger we remove the old record and insert it again.
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
	// Check if we are pointing to another record and recursively call
	if ( slot->IsOtherRecordTID() )
	{
		TID newTid = slot->GetOtherRecordTID();
		mBufferManager.UnfixPage( frame, true );
		return Update( newTid, r );
	}
	// Record is not on another page
	uint32_t offset = slot->GetOffset();
	uint32_t length = slot->GetLength();
	if ( slot->IsFromOtherPage() )
	{
		offset += 8;
		length -= 8;
	}

	if ( r.GetLen() == length )
	{
		// Same length entry, just overwrite no changes necessary
		memcpy( page->GetDataPointer( offset ), r.GetData(), r.GetLen() );
	}
	else if ( r.GetLen() < length )
	{
		// Smaller length entry, overwrite and change length
		memcpy( page->GetDataPointer( offset ), r.GetData(), r.GetLen() );
		uint32_t newlength = slot->IsFromOtherPage() ? r.GetLen() + 8 : r.GetLen();
		slot->SetLength( newlength );
	}
	else if ( slot->IsFromOtherPage() )
	{
		// Resolve the indirection, by completely deleting the intermediate one.
		TID backlink = page->GetBacklinkTID( offset - 8 );
		page->FreeSlot( pIdsId.second );
		page->FreeData( offset - 8, length + 8 );
		mBufferManager.UnfixPage( frame, true );
		return InsertLinked( backlink, r );
	}
	else
	{
		// Not from another page but still too big, so the current slot is our new backlink
		page->FreeData( offset, length );
		mBufferManager.UnfixPage( frame, true );
		return InsertLinked( tid, r );
	}
	mBufferManager.UnfixPage( frame, true );
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
		if ( !page->IsInitialized() || page->GetFreeContSpace() >= minSpace + 8 )
		{
			// Page is either a fresh page, which of course means we dont yet have any values set
			// this also means free space is not correct yet. But record has to fit, since
			// else it would throw above.
			mBufferManager.UnfixPage( frame, false );
			return curPage;
		}
		mBufferManager.UnfixPage( frame, false );
		++curPage;
	}
	assert( false );
	return 0;
}

/// <summary>
/// Inserts a record in an update step, where we have a backlink to a certain location, which also needs to be updated.
/// </summary>
/// <param name="backlink">The backlink.</param>
/// <param name="r">The r.</param>
/// <returns></returns>
bool SPSegment::InsertLinked( TID backlink, const Record& r )
{
	std::vector<uint8_t> data;
	data.resize( r.GetLen() + 8 );
	memcpy( &data[0], &backlink, 8 );
	memcpy( &data[8], r.GetData(), r.GetLen() );

	// Create copyrecord with backlink tid prepended
	Record copyR = Record( static_cast<uint32_t>(data.size()), &data[0] );

	// Insert this one with normal insert
	TID newTID = Insert( copyR, true );

	// Write tid into backlink
	std::pair<uint64_t, uint64_t> pIdsId = SplitTID( backlink );
	BufferFrame& frame = mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, pIdsId.first ), true );
	SlottedPage* page = reinterpret_cast<SlottedPage*>(frame.GetData());

	// Checks if tid is valid
	if ( !page->IsInitialized() )
	{
		mBufferManager.UnfixPage( frame, false );
		return false;
	}
	SlottedPage::Slot* slot = page->GetSlot( pIdsId.second );
	if ( !slot )
	{
		mBufferManager.UnfixPage( frame, false );
		return false;
	}
	slot->Overwrite( ~newTID ); // Have to remember, tids are inverted
	mBufferManager.UnfixPage( frame, true );
	return true;
}
