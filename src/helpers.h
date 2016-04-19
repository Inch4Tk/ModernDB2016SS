#pragma once
#ifndef HELPERS_H
#define HELPERS_H

#include <stdint.h>

void externalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize );
void assertCorrectOrderSort( const char* outputFilename );

#endif