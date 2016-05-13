#include "DBCore.h"

#include "utility/macros.h"
#include "utility/helpers.h"
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
	LoadSchemaFromSeg0();
}

/// <summary>
/// Finalizes an instance of the <see cref="DBCore"/> class.
/// </summary>
DBCore::~DBCore()
{
	WriteSchemaToSeg0();
	DeleteBufferManager();
}

/// <summary>
/// Wipes the database from disk. !NOT threadsafe. Deletes the buffer manager then deletes all the files and restores buffer manager.
/// </summary>
void DBCore::WipeDatabase()
{
	DeleteBufferManager();
	// We still have the most current masterschema in ram. 
	// We use this to identify all segments that need to be deleted
	if ( FileExists( "0" ) )
	{
		FileDelete( "0" );
	}
	for (Schema::Relation& r : mMasterSchema.relations)
	{
		if ( FileExists( std::to_string( r.segmentId ) ) )
		{
			FileDelete( std::to_string( r.segmentId ) );
		}
	}
	// Recreate Buffer manager
	mBufferManager = new BufferManager( 1000 );
	LoadSchemaFromSeg0();
}

/// <summary>
/// Adds relations to the masterschema from a file.
/// </summary>
/// <param name="filename">The filename.</param>
void DBCore::AddRelationsFromFile( const std::string& filename )
{
	SchemaParser parser( filename, false );
	std::unique_ptr<Schema> newSchema;
	try
	{
		newSchema = parser.parse();
	}
	catch ( SchemaParserError* e)
	{
		LogError( e->what() );
	}
	// Merge into master
	mMasterSchema.MergeSchema( *newSchema );
}


/// <summary>
/// Adds relations to the masterschema from a string.
/// </summary>
/// <param name="sql">The SQL.</param>
void DBCore::AddRelationsFromString( const std::string& sql )
{
	SchemaParser parser( sql, true );
	std::unique_ptr<Schema> newSchema;
	try
	{
		newSchema = parser.parse();
	}
	catch ( SchemaParserError* e )
	{
		LogError( e->what() );
	}
	// Merge into master
	mMasterSchema.MergeSchema( *newSchema );
}

/// <summary>
/// Gets the buffer manager.
/// </summary>
/// <returns></returns>
BufferManager* DBCore::GetBufferManager()
{
	return mBufferManager;
}

/// <summary>
/// Gets the schema.
/// </summary>
/// <returns></returns>
const Schema* DBCore::GetSchema()
{
	return &mMasterSchema;
}

/// <summary>
/// Releases Segment 0 and deletes the buffer manager.
/// </summary>
void DBCore::DeleteBufferManager()
{
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
/// Loads the schema from segment 0.
/// </summary>
void DBCore::LoadSchemaFromSeg0()
{
	// Secure exclusive access to the first page, first segment
	BufferFrame& f = mBufferManager->FixPage( 0, true );
	mSegment0.push_back( &f );
	uint8_t* startdata = reinterpret_cast<uint8_t*>(f.GetData());
	uint32_t segment0Pages = reinterpret_cast<uint32_t*>(startdata)[0];
	// Fix all additional segment 0 pages
	uint8_t* lastdata = startdata;
	for ( uint32_t i = 1; i < segment0Pages; ++i )
	{
		BufferFrame* nextF = &mBufferManager->FixPage( i, true );
		mSegment0.push_back( nextF );
		// Make sure that segment0 lies linearly mapped in memory
		// This should be guaranteed, since we just created the bm
		uint8_t* nextData = reinterpret_cast<uint8_t*>(nextF->GetData());
		assert( (lastdata + DB_PAGE_SIZE) == nextData );
		lastdata = nextData;
	}
	// Deserialize metadata
	startdata += 4; // Skip number of segments
	mMasterSchema.Deserialize( startdata );
}

/// <summary>
/// Writes the schema to segment 0.
/// </summary>
void DBCore::WriteSchemaToSeg0()
{
	// Serialize metadata/schema
	std::vector<uint8_t> schemaData;
	mMasterSchema.Serialize( schemaData );

	// Compute number of pages necessary to store and put that as first entry to segment 0
	uint32_t numpages = (static_cast<uint32_t>(schemaData.size()) / DB_PAGE_SIZE) + 1;
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
			std::min( static_cast<uint32_t>(schemaData.size()), DB_PAGE_SIZE - 4 ) );
	// Fix all extra pages necessary to store all metadata
	while ( mSegment0.size() < numpages )
	{
		BufferFrame* nextF = &mBufferManager->FixPage( mSegment0.size(), true );
		mSegment0.push_back( nextF );
	}
	// Insert other pages
	for ( uint32_t i = 1; i < numpages; ++i )
	{
		uint8_t* bufferdata = reinterpret_cast<uint8_t*>(mSegment0[i]->GetData());
		uint32_t rangestart = i * DB_PAGE_SIZE - 4;
		uint32_t rangeend = std::min( static_cast<uint32_t>(schemaData.size()),
									  (i + 1) * DB_PAGE_SIZE - 4 );
		memcpy( bufferdata, &schemaData[rangestart], rangeend - rangestart );
	}
}
