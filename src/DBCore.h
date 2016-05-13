#pragma once
#ifndef DBCORE_H
#define DBCORE_H

#include "sql/Schema.h"
#include "utility/defines.h"

#include <memory>
#include <stdint.h>

// Forwards
class BufferManager;
class BufferFrame;
class SPSegment;

/// <summary>
/// Database core class. Starts up all the internal things necessary for the database to function.
/// </summary>
class DBCore
{
public:
	DBCore();
	~DBCore();
	
	void WipeDatabase();
	void AddRelationsFromFile( const std::string& filename );
	void AddRelationsFromString( const std::string& sql );

	BufferManager* GetBufferManager();
	const Schema* GetSchema();
	uint64_t GetPagesOfRelation( uint64_t segmentId );
	uint64_t AddPagesToRelation( uint64_t segmentId, uint64_t numPages );
	std::unique_ptr<SPSegment> GetSPSegment( uint64_t segmentId );
	std::unique_ptr<SPSegment> GetSPSegment( std::string relationName );

private:
	RWLock mSchemaLock;
	Schema mMasterSchema;
	BufferManager* mBufferManager;
	std::vector<BufferFrame*> mSegment0; // Keeps all our writelocks on segment 0 pages

	void DeleteBufferManager();
	void LoadSchemaFromSeg0();
	void WriteSchemaToSeg0();
};

#endif