#pragma once
#ifndef HELPERS_H
#define HELPERS_H

#include <stdint.h>
#include <string>

// Logging functions
void LogDebug( const std::string& debugMessage );
void LogDebug( const std::wstring& debugMessage );
void LogError( const std::string& errorMessage );
void LogError( const std::wstring& errorMessage );
void Log( const std::string& message );
void Log( const std::wstring& message );

void ExternalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize );
void AssertCorrectOrderSort( const char* outputFilename );

#endif