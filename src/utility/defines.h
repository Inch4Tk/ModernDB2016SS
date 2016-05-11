#pragma once
#ifndef DEFINES_H
#define DEFINES_H

#ifdef __unix__      
#define PLATFORM_UNIX
#ifndef UNREFERENCED_PARAMETER
#define UNREFERENCED_PARAMETER(x) (void)x
#endif

#elif defined(_WIN32) || defined(WIN32) 
#define PLATFORM_WIN
#define NOMINMAX
// Memory leak detection for windows
#ifndef NDEBUG
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif // NDEBUG
#endif // Platform ifdef

#define DB_PAGE_SIZE 16384u
#define DB_EVICTION_COUNTER_START 0u
#define DB_TEST_SEGMENT UINT16_MAX
#include <stdint.h>
#define TID uint64_t

#endif