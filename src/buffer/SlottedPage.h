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
		// 1 Byte other record marker (if == 00000000b, slot points to record on this page, and status byte is valid)
		// 1 Byte status byte (0 = free slot, 1 = moved from other page, else = normal slot) 
		// 3 Byte offset in page (bytes)
		// 3 Byte length in page (bytes)
		// This performs some unaligned reads, well we just don't support platforms that can't do that
		// The other record TID is stored as a complement. This is useful, because it simplifies logic for our status byte.
		// (Pages start as all 0, so all slots are automatically all free.)
		
		// Setters
		void SetInPage();
		void SetOffset( uint32_t newOffset );
		void SetLength( uint32_t newLength );
		void MakeFree();
		void Overwrite( uint64_t newData );

		// Getters
		bool IsFree();
		bool IsOtherRecordTID();
		bool IsFromOtherPage();
		TID GetOtherRecordTID();
		uint32_t GetOffset();
		uint32_t GetLength();
	};

	// Setters etc
	void Initialize();
	void UsedFirstFreeSlot();
	void SetFirstFreeSlot(uint64_t slotId);
	void SetDataStart( uint32_t newDataStart );
	void FreeSlot( uint64_t slotId );
	void FreeData( uint32_t offset, uint32_t length );

	// Getters
	bool IsInitialized();
	uint16_t GetSlotCount();
	uint16_t GetFirstFreeSlotId();
	SlottedPage::Slot* GetFirstFreeSlot();
	uint32_t GetDataStart();
	uint32_t GetFreeContSpace();
	TID GetBacklinkTID( uint32_t offset );
	SlottedPage::Slot* GetSlot( uint64_t slotId );
	void* GetDataPointer( uint32_t offset );

private:
	uint8_t mData[DB_PAGE_SIZE];
	// Layout:
	// 2 Byte status (currently is only 0=uninitialized, >1=initialized)
	// 2 Byte slot count (not decremented on removal, this shows all the slots potentially used)
	// 2 Byte first free slot
	// 2 Byte padding, so we get 8 byte aligned slots and aligned
	// 4 Byte data start
	// 4 Byte free continuous space amt (currently we don't perform compactification, so storing compactified space is useless)
	// X * 8 Byte Slots
	// y Byte Data
	SlottedPage();
	~SlottedPage();
};

#endif