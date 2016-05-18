#pragma once
#ifndef BPTREE_H
#define BPTREE_H

#include "utility/defines.h"
#include <stdint.h>

/// <summary>
/// Parameterized B+ Tree implementation
/// </summary>
template <class T, typename CMP>
class BPTree
{
public:
	BPTree();
	~BPTree();

	bool Insert(T key, TID tid);
	bool Erase(T key);
	std::pair<bool, TID> Lookup(T key);

private:

};

/// <summary>
/// Inserts the specified key, TID tuple.
/// </summary>
/// <param name="key">The key.</param>
/// <param name="tid">The tid.</param>
/// <returns></returns>
template <class T, typename CMP>
bool BPTree<T, CMP>::Insert( T key, TID tid )
{

}

/// <summary>
/// Erases the specified key.
/// </summary>
/// <param name="key">The key.</param>
/// <returns></returns>
template <class T, typename CMP>
bool BPTree<T, CMP>::Erase( T key )
{

}

/// <summary>
/// Looks up the specified key. Indicates success in the first return value.
/// </summary>
/// <param name="key">The key.</param>
/// <returns></returns>
template <class T, typename CMP>
std::pair<bool, TID> BPTree<T, CMP>::Lookup( T key )
{

}

/// <summary>
/// Initializes a new instance of the <see cref="BPTree{T, CMP}"/> class.
/// </summary>
template <class T, typename CMP>
BPTree<T, CMP>::BPTree()
{

}

/// <summary>
/// Finalizes an instance of the <see cref="BPTree{T, CMP}"/> class.
/// </summary>
template <class T, typename CMP>
BPTree<T, CMP>::~BPTree()
{

}



#endif