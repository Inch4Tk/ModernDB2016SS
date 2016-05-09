#include "SlottedPage.h"

/// <summary>
/// Gets the slot count.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetSlotCount()
{
	return reinterpret_cast<uint16_t*>(mData)[0];
}

/// <summary>
/// Gets the first free slot.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetFirstFreeSlot()
{
	return reinterpret_cast<uint16_t*>(mData)[1];
}

/// <summary>
/// Gets the data start.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetDataStart()
{
	return reinterpret_cast<uint16_t*>(mData)[3];
}

/// <summary>
/// Gets the free space.
/// </summary>
/// <returns></returns>
uint16_t SlottedPage::GetFreeSpace()
{
	return reinterpret_cast<uint16_t*>(mData)[4];
}
