#include "SlottedPage.h"

#include <cassert>

/// <summary>
/// Initializes this instance.
/// </summary>
void SlottedPage::Initialize()
{
	mData[1] = 1; // Set initialized
	SetDataStart( DB_PAGE_SIZE ); // Update start and recalc space
}

/// <summary>
/// We used the first free slot, find next free slot. If the last free slot was a slot at the end, we increment the slot count.
/// Does not manipulate any data inside a slot (so slot has to be manually set to not free, e.g. with SetInPage() method)
/// </summary>
void SlottedPage::UsedFirstFreeSlot()
{

	uint16_t slotId = GetFirstFreeSlotId();
	// Increment slot count of last free slot was at the end.
	if ( slotId == GetSlotCount() )
	{
		++(reinterpret_cast<uint16_t*>(mData)[1]); // slot count
	}

	bool foundFreeSlot = false;
	do 
	{
		++slotId;
		SlottedPage::Slot* slot = GetSlot( slotId );
		if (!slot || slot->IsFree())
		{
			reinterpret_cast<uint16_t*>(mData)[2] = slotId; // first slot id
			foundFreeSlot = true;
		}
	} while ( !foundFreeSlot );
	
}

/// <summary>
/// Sets the first free slot if it is smaller than the current free slot.
/// </summary>
/// <param name="slotId">The slot identifier.</param>
void SlottedPage::SetFirstFreeSlot( uint64_t slotId )
{
	if ( static_cast<uint16_t>(slotId) < GetFirstFreeSlotId() )
	{
		reinterpret_cast<uint16_t*>(mData)[2] = static_cast<uint16_t>(slotId); // first slot id
		// We could also remove unused slots at the end to reduce slotcount.
		// But we will reuse these slots anyway later.
	}
}

/// <summary>
/// Sets the data start and recalculates free space.
/// </summary>
/// <param name="newDataStart">The new data start.</param>
void SlottedPage::SetDataStart( uint32_t newDataStart )
{
	assert( newDataStart >= static_cast<uint32_t>(16 + GetSlotCount() * 8) );
	reinterpret_cast<uint32_t*>(mData)[2] = newDataStart;
	uint32_t freeSpace = newDataStart - 16 - GetSlotCount() * 8;
	reinterpret_cast<uint32_t*>(mData)[3] = freeSpace;
}

/// <summary>
/// Frees the slot. Also writes in the slot. Does not clean up any records and does not manipulate header data related to records.
/// </summary>
/// <param name="slotId">The slot identifier.</param>
void SlottedPage::FreeSlot( uint64_t slotId )
{
	SlottedPage::Slot* slot = GetSlot( slotId );
	if ( !slot )
		return;
	slot->MakeFree();
	SetFirstFreeSlot( slotId );
}

/// <summary>
/// Frees the data at the spot. Length has to include the 8 bytes old tid, if the tuple was moved from another page.
/// Will only manipulate the space if the data was at the end of our datablock.
/// </summary>
/// <param name="offset">The offset.</param>
/// <param name="length">The length.</param>
void SlottedPage::FreeData( uint32_t offset, uint32_t length )
{
	if (offset == GetDataStart())
	{
		SetDataStart( offset + length );
	}
}

/// <summary>
/// Determines whether this instance is initialized. E.g. if the header is already initialized.
/// </summary>
/// <returns></returns>
bool SlottedPage::IsInitialized()
{
	return reinterpret_cast<uint16_t*>(mData)[0] > 0 ? true : false;
}

/// <summary>
/// Gets the slot count.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetSlotCount()
{
	return reinterpret_cast<uint16_t*>(mData)[1];
}

/// <summary>
/// Gets the first free slot identifier. The slot will stay a free slot have to manually make not free afterwards.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetFirstFreeSlotId()
{
	return reinterpret_cast<uint16_t*>(mData)[2];
}

/// <summary>
/// Gets the first free slot. The slot will stay a free slot have to manually make not free afterwards.
/// </summary>
/// <returns></returns>
SlottedPage::Slot* SlottedPage::GetFirstFreeSlot()
{
	uint16_t slotId = reinterpret_cast<uint16_t*>(mData)[2];
	return reinterpret_cast<SlottedPage::Slot*>(&mData[16 + slotId * 8]);
}

/// <summary>
/// Gets the data start.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::GetDataStart()
{
	return reinterpret_cast<uint32_t*>(mData)[2];
}

/// <summary>
/// Gets the free continuous space.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::GetFreeContSpace()
{
	return reinterpret_cast<uint32_t*>(mData)[3];
}

/// <summary>
/// Gets the backlink tid at the offset specified.
/// </summary>
/// <param name="offset">The offset.</param>
/// <returns></returns>
TID SlottedPage::GetBacklinkTID( uint32_t offset )
{
	return *reinterpret_cast<uint64_t*>(&mData[offset]);
}

/// <summary>
/// Gets the slot.
/// </summary>
/// <param name="slotId">The slot identifier.</param>
/// <returns></returns>
SlottedPage::Slot* SlottedPage::GetSlot( uint64_t slotId )
{
	if (GetSlotCount() <= slotId)
		return nullptr;
	return reinterpret_cast<SlottedPage::Slot*>(&mData[16 + slotId * 8]);
}

/// <summary>
/// Gets the data pointer.
/// </summary>
/// <param name="offset">The offset.</param>
/// <returns></returns>
void* SlottedPage::GetDataPointer( uint32_t offset )
{
	return &mData[offset];
}

/// <summary>
/// Sets the correct status bytes for a value directly inserted into the page and not moved.
/// </summary>
void SlottedPage::Slot::SetInPage()
{
	mData[0] = 0;
	mData[1] = 0xFF;
}

/// <summary>
/// Sets the correct status bytes for a value moved from another page.
/// </summary>
void SlottedPage::Slot::SetFromOtherPage()
{
	mData[0] = 0;
	mData[1] = 1;
}

/// <summary>
/// Sets the offset.
/// </summary>
/// <param name="newOffset">The new offset.</param>
void SlottedPage::Slot::SetOffset( uint32_t newOffset )
{
	// Mask the highest significant byte with value currently stored in
	// the data byte which we will overwrite.
	uint8_t oldD1 = mData[1];
	uint32_t combinedOffset = (newOffset << 8) & 0xFFFFFFF00;
	combinedOffset |= static_cast<uint32_t>(mData[1]);
	*reinterpret_cast<uint32_t*>(&mData[1]) = combinedOffset;
	assert( mData[1] == oldD1 );
}

/// <summary>
/// Sets the length.
/// </summary>
/// <param name="newLength">The new length.</param>
void SlottedPage::Slot::SetLength( uint32_t newLength )
{
	// Mask the highest significant byte with value currently stored in
	// the data byte which we will overwrite.
	uint8_t oldD4 = mData[4];
	uint32_t combinedLength = (newLength << 8) & 0xFFFFFFF00;
	combinedLength |= static_cast<uint32_t>(mData[4]);
	*reinterpret_cast<uint32_t*>(&mData[4]) = combinedLength;
	assert( mData[4] == oldD4 );
}

/// <summary>
/// Makes the slot completely empty
/// </summary>
void SlottedPage::Slot::MakeFree()
{
	*reinterpret_cast<uint64_t*>(mData) = 0x0000000000000000;
}

/// <summary>
/// Overwrites the slot with specified new data.
/// </summary>
/// <param name="newData">The new data.</param>
void SlottedPage::Slot::Overwrite( uint64_t newData )
{
	*reinterpret_cast<uint64_t*>(mData) = newData;
}

/// <summary>
/// Determines whether this instance is free.
/// </summary>
/// <returns></returns>
bool SlottedPage::Slot::IsFree()
{
	if ( IsOtherRecordTID() )
	{
		return false;
	}
	return mData[1] == 0 ? true : false;
}

/// <summary>
/// Determines whether [is other record tid].
/// </summary>
/// <returns></returns>
bool SlottedPage::Slot::IsOtherRecordTID()
{
	return mData[0] == 0x00 ? false : true;
}

/// <summary>
/// Determines whether [is from other page]. This is not valid if the slot contains another record's TID.
/// </summary>
/// <returns></returns>
bool SlottedPage::Slot::IsFromOtherPage()
{
	return mData[1] == 1 ? true : false;
}

/// <summary>
/// Gets the other record tid.
/// </summary>
/// <returns></returns>
TID SlottedPage::Slot::GetOtherRecordTID()
{
	TID t = *reinterpret_cast<uint64_t*>(mData);
	t = ~t; // Other record tids are stored in complement form, see comments in slot
	return t;
}

/// <summary>
/// Gets the offset. Returns the exact data begin, including any extra offset for the backlink tid.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::Slot::GetOffset()
{
	uint32_t offset = *reinterpret_cast<uint32_t*>(&mData[1]);
	offset &= 0xFFFFFF00; // Mask away the first byte, since this is actually not part of offset
	return offset >> 8;
}

/// <summary>
/// Gets the length. Returns the whole length, including any length that is for the backlink tid.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::Slot::GetLength()
{
	uint32_t length = *reinterpret_cast<uint32_t*>(&mData[4]);
	length &= 0xFFFFFF00; // Mask away the first byte, since this is actually not part of length
	return length >> 8;
}
