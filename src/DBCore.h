#pragma once
#ifndef DBCORE_H
#define DBCORE_H

#include "sql/Schema.h"
#include "utility/defines.h"

#include <stdint.h>

// Forwards
class BufferManager;
class BufferFrame;

/// <summary>
/// Database core class. Starts up all the internal things necessary for the database to function.
/// </summary>
class DBCore
{
public:
	DBCore();
	~DBCore();
	
	void AddRelationsFromFile( const std::string& filename );
	void AddRelationsFromString( const std::string& sql );

	BufferManager* GetBufferManager();
	const Schema* GetSchema();

private:
	Schema mMasterSchema;
	BufferManager* mBufferManager;
	std::vector<BufferFrame*> mSegment0; // Keeps all our writelocks on segment 0 pages
};

#endif