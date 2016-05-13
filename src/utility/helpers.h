#pragma once
#ifndef HELPERS_H
#define HELPERS_H

#include "utility/defines.h"
#include <stdint.h>
#include <string>

// Logging functions
void LogDebug( const std::string& debugMessage );
void LogDebug( const std::wstring& debugMessage );
void LogError( const std::string& errorMessage );
void LogError( const std::wstring& errorMessage );
void Log( const std::string& message );
void Log( const std::wstring& message );

// Filesystem specials
bool FileExists( const std::string& filename );
void FileDelete( const std::string& filename );

// Tuple id manipulation
std::pair<uint64_t, uint64_t> SplitTID( TID toSplit );
TID MergeTID( uint64_t pageId, uint64_t slotId );

// Old
void ExternalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize );
void AssertCorrectOrderSort( const char* outputFilename );

#endif