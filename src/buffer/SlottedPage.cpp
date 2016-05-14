#include "SlottedPage.h"

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
/// Determines whether this instance is free.
/// </summary>
/// <returns></returns>
bool SlottedPage::Slot::IsFree()
{
	if ( IsOtherRecordTID() )
	{
		return false;
	}
	return mData[0] == 0 ? true : false;
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
	return mData[0] == 1 ? false : true;
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
