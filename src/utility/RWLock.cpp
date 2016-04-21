#include "RWLock.h"

/// <summary>
/// Initializes a new instance of the <see cref="RWLock"/> class.
/// </summary>
RWLock::RWLock()
{
#ifdef PLATFORM_WIN
	InitializeSRWLock( &mRwlock );
#endif
}

/// <summary>
/// Finalizes an instance of the <see cref="RWLock"/> class.
/// </summary>
RWLock::~RWLock()
{

}

/// <summary>
/// Tries to acquire write lock (now). Returns true if succeeded, false if failed.
/// </summary>
/// <returns></returns>
bool RWLock::TryLockWrite()
{
	// TODO:
}

/// <summary>
/// Locks for writing.
/// </summary>
void RWLock::LockWrite()
{
#ifdef PLATFORM_WIN
	AcquireSRWLockExclusive( &mRwlock );
#else
	pthread_rwlock_wrlock( &mRwlock );
#endif
}

/// <summary>
/// Locks for reading (shared access among readers).
/// </summary>
void RWLock::LockRead()
{
#ifdef PLATFORM_WIN
	AcquireSRWLockShared( &mRwlock );
#else
	pthread_rwlock_rdlock( &mRwlock );
#endif
}

/// <summary>
/// Unlocks for writing (Call if thread acquired a write lock).
/// </summary>
void RWLock::UnlockWrite()
{
#ifdef PLATFORM_WIN
	ReleaseSRWLockExclusive( &mRwlock );
#else
	pthread_rwlock_unlock( &mRwlock );
#endif
}

/// <summary>
/// Unlocks for reading (Call if thread acquired a read lock).
/// </summary>
void RWLock::UnlockRead()
{
#ifdef PLATFORM_WIN
	ReleaseSRWLockShared( &mRwlock );
#else
	pthread_rwlock_unlock( &mRwlock );
#endif
}
