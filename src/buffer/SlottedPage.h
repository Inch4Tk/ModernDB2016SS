#pragma once
#ifndef SLOTTED_PAGE_H
#define SLOTTED_PAGE_H

#include "utility/defines.h"

#include <stdint.h>

/// <summary>
/// Slotted page interface class
/// </summary>
class SlottedPage
{
public:
	uint16_t GetSlotCount();
	uint16_t GetFirstFreeSlot();
	uint16_t GetDataStart();
	uint16_t GetFreeSpace();

private:
	uint8_t mData[DB_PAGE_SIZE];
	// Layout:
	// 2 Byte slot count
	// 2 Byte first free slot
	// 2 Byte data start
	// 2 Byte free space amt
	// x Byte Slots
	// y Byte Data
	SlottedPage();
	~SlottedPage();
};

#endif