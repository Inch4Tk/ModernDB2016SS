#include "TableScanOperator.h"

#include "utility/macros.h"
#include "Register.h"
#include "DBCore.h"
#include "buffer/BufferManager.h"
#include "buffer/BufferFrame.h"
#include "buffer/SlottedPage.h"

#include <cassert>

TableScanOperator::TableScanOperator( const std::string& relationName, DBCore& core, BufferManager& bm ) : 
	mCore(core), mBufferManager(bm)
{
	mSegmentId = mCore.GetSegmentIdOfRelation(relationName);
}

TableScanOperator::TableScanOperator( uint64_t segmentId, DBCore& core, BufferManager& bm ):
	mCore( core ), mBufferManager( bm ), mSegmentId( segmentId )
{

}

TableScanOperator::~TableScanOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void TableScanOperator::Open()
{
	// Create registers depending on the schema
	// First check if there are already registers existing
	for ( Register* r : mRegisters )
	{
		SDELETE( r );
	}
	mRegisters.clear();
	// Thread safe fetch of attributes (can be changed after fetching though, but that would violate the principles anyways)
	std::vector<Schema::Relation::Attribute> attributes = mCore.GetRelationAttributes( mSegmentId );
	for ( Schema::Relation::Attribute& a : attributes)
	{
		mRegisters.push_back( new Register() );
		mRegisters.back()->mType = a.type;
		mRegisters.back()->mAttrName = a.name;
	}

	// Other init work
	if ( mCurFrame )
	{
		mBufferManager.UnfixPage( *mCurFrame, false );
	}
	mCurPageId = 0;
	mCurSlot = 0;
	mCurFrame = &mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, mCurPageId ), false );
}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool TableScanOperator::Next()
{
	uint64_t pagecount = mCore.GetPagesOfRelation( mSegmentId );
	SlottedPage* sp = reinterpret_cast<SlottedPage*>(mCurFrame->GetData());
	assert( sp->IsInitialized() );
	if ( mCurPageId + 1 < pagecount && mCurSlot >= sp->GetSlotCount() )
	{
		// We are still inside the segment bounds but there are no more slots in our page
		// so we grab the next page
		mBufferManager.UnfixPage( *mCurFrame, false );
		++mCurPageId;
		mCurSlot = 0;
		mCurFrame = &mBufferManager.FixPage( BufferManager::MergePageId( mSegmentId, mCurPageId ), false );
		sp = reinterpret_cast<SlottedPage*>(mCurFrame->GetData());
	}
	else if ( mCurPageId + 1 >= pagecount && mCurSlot >= sp->GetSlotCount() )
	{
		// on the last page and out of slots return false
		return false;
	}
	
	assert( sp->IsInitialized() );
	// Walk over slots until we find a valid slot or are out of bounds
	while ( mCurSlot < sp->GetSlotCount() )
	{
		SlottedPage::Slot* slot = sp->GetSlot( mCurSlot );
		++mCurSlot; // Already increment here so we can use continue

		// We skip all free slots and slots that contain a tid
		// We skip the tid slots because they are not on this page, but we get them anyways, because we walk over all pages
		if ( slot->IsFree() || slot->IsOtherRecordTID() )
			continue;

		uint32_t exOffset = 0;
		if (slot->IsFromOtherPage())
		{
			exOffset = 8; // compensate backlink tid
		}

		// Got a valid slot, read values to register and return
		TupleToRegisters( reinterpret_cast<uint8_t*>(sp->GetDataPointer( slot->GetOffset() + exOffset )), slot->GetLength() - exOffset );
		return true;
	}

	return Next(); // if we got here without returning it means we ran out of slots on our page, 
				   // to not unnecessarily repeat logic for fetching new pages we restart.
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> TableScanOperator::GetOutput()
{
	return mRegisters;
}

/// <summary>
/// Closes this instance.
/// </summary>
void TableScanOperator::Close()
{
	for ( Register* r : mRegisters )
	{
		SDELETE( r );
	}
	mRegisters.clear();
	if( mCurFrame )
	{
		mBufferManager.UnfixPage( *mCurFrame, false );
		mCurFrame = nullptr;
	}
	mCurPageId = 0;
	mCurSlot = 0;
	// Delete registers
}

/// <summary>
/// Writes the tuples to registers
/// </summary>
/// <param name="datastart">The datastart.</param>
/// <param name="size">The size.</param>
void TableScanOperator::TupleToRegisters( uint8_t* data, uint32_t size )
{
	// The table scan assumes that EVERY thing with char type was stored with a 4 byte length preceding the string and without a terminating 0
	// And that everything with int type was stored with 4 byte size.
	uint8_t* originalStart = data;
	for (Register* r : mRegisters)
	{
		if ( r->GetType() == SchemaTypes::Tag::Integer )
		{
			r->mIntVar = *reinterpret_cast<const uint32_t*>(data);
			data += 4;
		}
		else if ( r->GetType() == SchemaTypes::Tag::Char )
		{
			uint32_t strlen = 0;
			strlen = *reinterpret_cast<const uint32_t*>(data);
			data += 4;
			r->mStringVar = std::string( reinterpret_cast<const char*>(data), strlen );
			data += strlen;
		}
		else
		{
			assert( false );
		}
	}

	assert( data - originalStart == size ); // Just for control
}
