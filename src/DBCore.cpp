#include "DBCore.h"

#include "utility/macros.h"
#include "utility/helpers.h"
#include "sql/SchemaParser.h"
#include "buffer/SPSegment.h"
#include "buffer/BufferManager.h"

#include <algorithm>
#include <cassert>

/// <summary>
/// Initializes a new instance of the <see cref="DBCore"/> class.
/// </summary>
DBCore::DBCore()
{
	mBufferManager = new BufferManager(5000);
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
	for ( uint32_t i = 0; i < 50; ++i )
	{
		// Just delete some of the first segments, if there are any 
		// later segments, these need to be found via schema
		std::string strname = std::to_string( i );
		if ( FileExists( strname ) )
		{
			FileDelete( strname );
		}
	}
	
	for (Schema::Relation& r : mMasterSchema.relations)
	{
		if ( FileExists( std::to_string( r.segmentId ) ) )
		{
			FileDelete( std::to_string( r.segmentId ) );
		}

		for ( Schema::Relation::Index& i : r.indices )
		{
			if ( FileExists( std::to_string( i.segmentId ) ) )
			{
				FileDelete( std::to_string( i.segmentId ) );
			}
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
	// TODO: locking
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
	mSchemaLock.LockWrite();
	mMasterSchema.MergeSchema( *newSchema );
	mSchemaLock.UnlockWrite();
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
	mSchemaLock.LockWrite();
	mMasterSchema.MergeSchema( *newSchema );
	mSchemaLock.UnlockWrite();
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
/// Gets the schema. Does not guarantee any threadsafety on reading the schema.
/// </summary>
/// <returns></returns>
const Schema* DBCore::GetSchema()
{
	return &mMasterSchema;
}

/// <summary>
/// Performs a threadsafe attribute fetch and copies them to a return vector.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <returns></returns>
std::vector<Schema::Relation::Attribute> DBCore::GetRelationAttributes( uint64_t segmentId )
{
	std::vector<Schema::Relation::Attribute> vec;
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithSegmentId( segmentId );
		for ( Schema::Relation::Attribute& a : r.attributes )
		{
			vec.push_back( a );
		}
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return vec;
}

/// <summary>
/// Gets the pages of a relation. Threadsafe iteration through schema. But number of pages can increase after return.
/// Throws on non-existent relation
/// </summary>
/// <returns></returns>
uint64_t DBCore::GetPagesOfRelation( uint64_t segmentId )
{
	uint64_t pagecount = 0;
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithSegmentId( segmentId );
		pagecount = r.pagecount;
	}
	catch (std::runtime_error& e)
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return pagecount;
}

/// <summary>
/// Adds x new pages to a relation.
/// Throws on non-existent relation.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <param name="numPages">The number pages.</param>
/// <returns>Returns the pageid of the last page added (already containing segmentid)</returns>
uint64_t DBCore::AddPagesToRelation( uint64_t segmentId, uint64_t numPages )
{
	uint64_t pageId = 0;
	mSchemaLock.LockWrite();
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithSegmentId( segmentId );
		r.pagecount += numPages;
		pageId = BufferManager::MergePageId( segmentId, r.pagecount - 1 );
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockWrite();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockWrite();
	return pageId;
}

/// <summary>
/// Gets the pages of an index. Threadsafe iteration through schema. But number of pages can increase after return.
/// Throws on non-existent index
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <returns></returns>
uint64_t DBCore::GetPagesOfIndex( uint64_t segmentId )
{
	uint64_t pagecount = 0;
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation::Index& i = mMasterSchema.GetIndexWithSegmentId( segmentId );
		pagecount = i.pagecount;
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return pagecount;
}

/// <summary>
/// Adds x new pages to an index.
/// Throws on non-existent index.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <param name="numPages">The number pages.</param>
/// <returns>Returns the pageid of the last page added (already containing segmentid)</returns>
uint64_t DBCore::AddPagesToIndex( uint64_t segmentId, uint64_t numPages )
{
	uint64_t pageId = 0;
	mSchemaLock.LockWrite();
	try
	{
		Schema::Relation::Index& i = mMasterSchema.GetIndexWithSegmentId( segmentId );
		i.pagecount += numPages;
		pageId = BufferManager::MergePageId( segmentId, i.pagecount - 1 );
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockWrite();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockWrite();
	return pageId;
}

/// <summary>
/// Gets the root pageid of the index specified by segmentId. Verify that the returned page is still root after fixing with buffer manager.
/// Throws on non-existent index.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <returns>Returns the pageid of root page (already containing segmentid)</returns>
uint64_t DBCore::GetRootOfIndex( uint64_t segmentId )
{
	uint64_t rootId = 0;
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation::Index& i = mMasterSchema.GetIndexWithSegmentId( segmentId );
		rootId = i.rootId;
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return rootId;
}

/// <summary>
/// Sets the root page of the index specified by segmentid to rootId. Root id has to contain the segmentid already. 
/// Only set the root if both the old root and the new root are already write fixed pages.
/// Throws on non-existent index.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <param name="rootId">The root identifier.</param>
void DBCore::SetRootOfIndex( uint64_t segmentId, uint64_t rootId )
{
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation::Index& i = mMasterSchema.GetIndexWithSegmentId( segmentId );
		i.rootId = rootId;
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
}

/// <summary>
/// Gets a new slotted pages segment instance operating on segment with segmentId.
/// Getting a segment of a non-existent relation will throw.
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <returns></returns>
std::unique_ptr<SPSegment> DBCore::GetSPSegment( uint64_t segmentId )
{
	mSchemaLock.LockRead();
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithSegmentId( segmentId );
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return std::move( std::unique_ptr<SPSegment>( new SPSegment( *this, *mBufferManager, segmentId ) ) );
}

/// <summary>
/// Gets a new slotted pages segment instance operating on the segment of the relation with the provided name.
/// Getting a segment of a non-existent relation will throw.
/// </summary>
/// <param name="relationName">Name of the relation.</param>
/// <returns></returns>
std::unique_ptr<SPSegment> DBCore::GetSPSegment( const std::string& relationName )
{
	mSchemaLock.LockRead();
	std::unique_ptr<SPSegment> s(nullptr);
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithName( relationName );
		s.reset( new SPSegment( *this, *mBufferManager, r.segmentId ) );
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return std::move( s );
}

/// <summary>
/// Gets the segment identifier of relation.
/// </summary>
/// <param name="relationName">Name of the relation.</param>
/// <returns></returns>
uint64_t DBCore::GetSegmentIdOfRelation( const std::string& relationName )
{
	mSchemaLock.LockRead();
	uint64_t id = 0;
	try
	{
		Schema::Relation& r = mMasterSchema.GetRelationWithName( relationName );
		id = r.segmentId;
	}
	catch( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return id;
}

/// <summary>
/// Gets the segment id of the index to attribute with attributeName in relation relationName.
/// </summary>
/// <param name="relationName">Name of the relation.</param>
/// <param name="attributeName">Name of the attribute.</param>
/// <returns></returns>
uint64_t DBCore::GetSegmentOfIndex( const std::string& relationName, const std::string& attributeName )
{
	mSchemaLock.LockRead();
	uint64_t id = 0;
	try
	{
		Schema::Relation::Index& i = mMasterSchema.GetIndexWithName( relationName, attributeName );
		id = i.segmentId;
	}
	catch ( std::runtime_error& e )
	{
		mSchemaLock.UnlockRead();
		throw std::runtime_error( e.what() );
	}
	mSchemaLock.UnlockRead();
	return id;
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
	mSchemaLock.LockWrite();
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
	mSchemaLock.UnlockWrite();
}

/// <summary>
/// Writes the schema to segment 0.
/// </summary>
void DBCore::WriteSchemaToSeg0()
{
	mSchemaLock.LockWrite();
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
	mSchemaLock.UnlockWrite();
}
