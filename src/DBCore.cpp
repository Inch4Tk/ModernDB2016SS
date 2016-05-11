#include "DBCore.h"

#include "utility/macros.h"
#include "sql/SchemaParser.h"
#include "buffer/BufferManager.h"

#include <algorithm>
#include <cassert>

/// <summary>
/// Initializes a new instance of the <see cref="DBCore"/> class.
/// </summary>
DBCore::DBCore()
{
	mBufferManager = new BufferManager(1000);
	// Secure exclusive access to the first page, first segment
	BufferFrame& f = mBufferManager->FixPage( 0, true );
	mSegment0.push_back( &f );
	uint8_t* startdata = reinterpret_cast<uint8_t*>(f.GetData());
	uint32_t segment0Pages = reinterpret_cast<uint32_t*>(startdata)[0];
	// Fix all additional segment 0 pages
	uint8_t* lastdata = startdata;
	for ( uint32_t i = 1; i < segment0Pages; ++i)
	{
		BufferFrame* nextF = &mBufferManager->FixPage( i, true );
		mSegment0.push_back( nextF );
		// Make sure that segment0 lies linearly mapped in memory
		// This should be guaranteed, since we just created the bm
		uint8_t* nextData = reinterpret_cast<uint8_t*>(f.GetData());
		assert( (lastdata + DB_PAGE_SIZE) == nextData );
		lastdata = nextData;
	}
	// Deserialize metadata
	startdata += 4; // Skip number of segments
	mMasterSchema.Deserialize( startdata );
}

/// <summary>
/// Finalizes an instance of the <see cref="DBCore"/> class.
/// </summary>
DBCore::~DBCore()
{
	// Serialize metadata/schema
	std::vector<uint8_t> schemaData;
	mMasterSchema.Serialize( schemaData );

	// Compute number of pages necessary to store and put that as first entry to segment 0
	uint32_t numpages = (schemaData.size() / DB_PAGE_SIZE) + 1;
	uint8_t* ptr = reinterpret_cast<uint8_t*>(&numpages);
	uint8_t* startdata = reinterpret_cast<uint8_t*>(mSegment0[0]->GetData());
	startdata[0] = ptr[0];
	startdata[1] = ptr[1];
	startdata[2] = ptr[2];
	startdata[3] = ptr[3];
	
	// Store schema in segment0, this time we can not be sure about sequential ram mapping
	// since segment0, might have grown (new relations)
	// First page
	memcpy( startdata + 4, &schemaData[0], 
			std::min(static_cast<uint32_t>(schemaData.size()), DB_PAGE_SIZE - 4 ) );
	// Fix all extra pages necessary to store all metadata
	while (mSegment0.size() < numpages)
	{
		BufferFrame* nextF = &mBufferManager->FixPage( mSegment0.size(), true );
		mSegment0.push_back( nextF );
	}
	// Insert other pages
	for ( uint32_t i = 1; i < numpages; ++i)
	{
		uint8_t* bufferdata = reinterpret_cast<uint8_t*>(mSegment0[i]->GetData());
		uint32_t rangestart = i * DB_PAGE_SIZE - 4;
		uint32_t rangeend = std::min( static_cast<uint32_t>(schemaData.size()), 
									  (i + 1) * DB_PAGE_SIZE - 4 );
		memcpy( bufferdata, &schemaData[rangestart], rangeend - rangestart );
	}

	// Unfix segment0
	for ( BufferFrame* frame : mSegment0 )
	{
		mBufferManager->UnfixPage( *frame, true );
	}
	mSegment0.clear();

	// Delete components
	SDELETE( mBufferManager );
}

/// <summary>
/// Gets the buffer manager.
/// </summary>
/// <returns></returns>
BufferManager* DBCore::GetBufferManager()
{
	return mBufferManager;
}
