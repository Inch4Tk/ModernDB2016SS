#include "SlottedPage.h"

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
		++(reinterpret_cast<uint16_t*>(mData)[1]);
	}

	bool foundFreeSlot = false;
	do 
	{
		++slotId;
		SlottedPage::Slot* slot = GetSlot( slotId );
		if (slot->IsFree())
		{
			reinterpret_cast<uint16_t*>(mData)[2] = slotId;
			foundFreeSlot = true;
		}
	} while ( !foundFreeSlot );
	
}

/// <summary>
/// Sets the data start and recalculates free space.
/// </summary>
/// <param name="newDataStart">The new data start.</param>
void SlottedPage::SetDataStart( uint32_t newDataStart )
{
	reinterpret_cast<uint32_t*>(mData)[3] = newDataStart;
	uint32_t freeSpace = newDataStart - 16 - GetSlotCount() * 8;
	reinterpret_cast<uint32_t*>(mData)[4] = freeSpace;
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
/// Sets the correct status bytes for a value directly inserted into the page and not moved.
/// </summary>
void SlottedPage::Slot::SetInPage()
{
	mData[0] = 0;
	mData[1] = 0xFF;
}

/// <summary>
/// Sets the offset.
/// </summary>
/// <param name="newOffset">The new offset.</param>
void SlottedPage::Slot::SetOffset( uint32_t newOffset )
{
	// Mask the highest significant byte with value currently stored in
	// the data byte which we will overwrite.
	newOffset &= static_cast<uint32_t>(mData[1]) << 24;
	*reinterpret_cast<uint32_t*>(&mData[1]) = newOffset;
}

/// <summary>
/// Sets the length.
/// </summary>
/// <param name="newLength">The new length.</param>
void SlottedPage::Slot::SetLength( uint32_t newLength )
{
	// Mask the highest significant byte with value currently stored in
	// the data byte which we will overwrite.
	newLength &= static_cast<uint32_t>(mData[4]) << 24;
	*reinterpret_cast<uint32_t*>(&mData[4]) = newLength;
}

/// <summary>
/// Makes the slot completely empty
/// </summary>
void SlottedPage::Slot::MakeFree()
{
	*reinterpret_cast<uint64_t*>(mData) = 0x0000000000000000;
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
	return mData[1] == 1 ? false : true;
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
/// Gets the offset.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::Slot::GetOffset()
{
	uint32_t offset = *reinterpret_cast<uint32_t*>(&mData[1]);
	offset &= 0x00FFFFFF; // Mask away the first byte, since this is actually not part of offset
	return offset;
}

/// <summary>
/// Gets the length.
/// </summary>
/// <returns></returns>
uint32_t SlottedPage::Slot::GetLength()
{
	uint32_t length = *reinterpret_cast<uint32_t*>(&mData[4]);
	length &= 0x00FFFFFF; // Mask away the first byte, since this is actually not part of length
	return length;
}
