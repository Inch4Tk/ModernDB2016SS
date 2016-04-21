#pragma once
#ifndef RWLOCK_H
#define RWLOCK_H

#include "utility/defines.h"

#ifndef PLATFORM_WIN
#include <pthread.h>
#else
#include <WinSock2.h>
#endif

/// <summary>
/// Cross platform read write lock wrapper. Uses SRW lock on windows and pthread_rwlock_t on unix.
/// </summary>
class RWLock
{
public:
	RWLock();
	~RWLock();

	bool TryLockWrite();
	void LockWrite();
	void LockRead();
	void UnlockWrite();
	void UnlockRead();
private:
#ifdef PLATFORM_WIN
	SRWLOCK mRwlock;
#else
	pthread_rwlock_t  mRwlock = PTHREAD_RWLOCK_INITIALIZER;
#endif

};

#endif