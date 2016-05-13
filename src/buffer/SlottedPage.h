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
	struct Slot
	{
		uint8_t mData[8];
		// Layout:
		// 1 Byte other record marker (if == 11111111b, slot points to on page entry)
		// 1 Byte status byte (0 = free slot, 1 = moved from other page, else = normal slot) 
		// 3 Byte offset in page (bytes)
		// 3 Byte length in page (bytes)
		// This performs some unaligned reads, well we just don't support platforms that can't do that
		bool IsFree();
		bool IsOtherRecordTID();
		bool IsFromOtherPage();
		TID GetOtherRecordTID();
		uint32_t GetOffset();
		uint32_t GetLength();
	};

	bool IsInitialized();
	uint16_t GetSlotCount();
	uint16_t GetFirstFreeSlot();
	uint16_t GetDataStart();
	uint16_t GetFreeSpace();
	SlottedPage::Slot* GetSlot( uint64_t slotId );
private:
	uint8_t mData[DB_PAGE_SIZE];
	// Layout:
	// 2 Byte status (currently is only 0=uninitialized, >1=initialized)
	// 2 Byte slot count
	// 2 Byte first free slot
	// 2 Byte data start
	// 2 Byte free space amt
	// 6 Byte currently leftover space, so we get 8 byte aligned slots 
	// (probably not necessary, but w/e, 6 bytes, maybe we find a use later)
	// X * 8 Byte Slots
	// y Byte Data
	SlottedPage();
	~SlottedPage();
};

#endif