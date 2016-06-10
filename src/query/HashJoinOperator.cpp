#include "HashJoinOperator.h"

#include "utility/macros.h"
#include "Register.h"
#include <cassert>

HashJoinOperator::HashJoinOperator( QueryOperator& inputLeft, QueryOperator& inputRight, std::string leftAttrName, std::string rightAttrName )
	: mInputLeft( inputLeft ), mInputRight( inputRight ), mLeftAttrName( leftAttrName ), mRightAttrName( rightAttrName )
{

}

HashJoinOperator::~HashJoinOperator()
{
}

/// <summary>
/// Opens this instance.
/// </summary>
void HashJoinOperator::Open()
{
	// Empty all hashmaps
	mHashInt.clear();
	mHashString.clear();
	mIteratorOffest = 0;

	// Empty all registers
	for ( uint32_t i = 0; i < mInputRegisterLeft.size(); ++i )
	{
		SDELETE( mOutputRegister[i] );
	}
	mOutputRegister.clear();
	mInputRegisterLeft.clear();
	mInputRegisterRight.clear();

	mInputLeft.Open();
	mInputRegisterLeft = mInputLeft.GetOutput();
	// Detect left side comparison attribute
	uint32_t curidx = 0;
	bool found = false;
	for ( Register* r : mInputRegisterLeft )
	{
		if ( r->GetAttributeName() == mLeftAttrName )
		{
			found = true;
			mLeftId = curidx;
			if ( r->GetType() == SchemaTypes::Tag::Char )
				mAttrIsString = true;
			else
				mAttrIsString = false;
			break;
		}
		++curidx;
	}
	assert( found );
	// Prepare output registers associated with left side
	// these we have to build ourselves
	for ( Register* r : mInputRegisterLeft )
	{
		mOutputRegister.push_back( new Register() );
		mOutputRegister.back()->mType = r->GetType();
		mOutputRegister.back()->mAttrName = r->GetAttributeName();
	}

	// Go over left input and store it completely in the hashmap
	while (mInputLeft.Next())
	{
		std::vector<Register> stored;
		for ( Register* r : mInputRegisterLeft )
		{
			stored.push_back( *r );
		}
		if (mAttrIsString)
			mHashString.insert( std::make_pair( mInputRegisterLeft[mLeftId]->GetString(), stored ) );
		else
			mHashInt.insert( std::make_pair( mInputRegisterLeft[mLeftId]->GetInteger(), stored ) );
	}
	mInputLeft.Close();

	// Prepare right side
	mInputRight.Open();
	mInputRegisterRight = mInputRight.GetOutput();
	curidx = 0;
	found = false;
	for ( Register* r : mInputRegisterRight )
	{
		if ( r->GetAttributeName() == mRightAttrName )
		{
			found = true;
			mRightId = curidx;
			if ( mAttrIsString )
				assert( r->GetType() == SchemaTypes::Tag::Char );
			else
				assert( r->GetType() == SchemaTypes::Tag::Integer );
			break;
		}
		++curidx;
	}
	assert( found );
	// Prepare output registers associated with right side
	// Leave out the register containing the right side attribute since that is already included in the left side
	for ( Register* r : mInputRegisterRight )
	{
		if ( r->GetAttributeName() != mRightAttrName )
		{
			mOutputRegister.push_back( r );
		}
	}
}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool HashJoinOperator::Next()
{
	// Our hash maps are multimaps, so we cache two iterators and produce pairs until they are equal, then we go to the next right side value
	if (mAttrIsString)
	{
		auto it = mHashString.equal_range( mInputRegisterRight[mRightId]->GetString() );

		// Compare our range iterators, if they are unequal we produce another tuple with the first 
		// and the current right side and increment offset. 
		// If they are equal, we advance the right side and reset the offset and restart
		for ( uint32_t i = 0; i < mIteratorOffest; ++i )
			++it.first;
		if ( it.first != it.second )
		{
			// Copy cached register over
			std::vector<Register>& regs = it.first->second;
			for ( uint32_t i = 0; i < regs.size(); ++i )
			{
				*(mOutputRegister[i]) = regs[i];
			}
			++mIteratorOffest;
			return true;
		}
		else
		{
			mIteratorOffest = 0;
			if (mInputRight.Next())
			{
				return Next();
			}
			else
			{
				return false;
			}
		}
	}
	else
	{
		// Same as above, sadly it is not really trivial to remove the duplicate code, since iterator is with auto and different types
		auto it = mHashInt.equal_range( mInputRegisterRight[mRightId]->GetInteger() );

		// Compare our range iterators, if they are unequal we produce another tuple with the first 
		// and the current right side and increment offset. 
		// If they are equal, we advance the right side and reset the offset and restart
		for ( uint32_t i = 0; i < mIteratorOffest; ++i )
			++it.first;
		if ( it.first != it.second )
		{
			// Copy cached register over
			std::vector<Register>& regs = it.first->second;
			for ( uint32_t i = 0; i < regs.size(); ++i )
			{
				*(mOutputRegister[i]) = regs[i];
			}
			++mIteratorOffest;
			return true;
		}
		else
		{
			mIteratorOffest = 0;
			if ( mInputRight.Next() )
			{
				return Next();
			}
			else
			{
				return false;
			}
		}
	}

	assert( false );
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> HashJoinOperator::GetOutput()
{
	return mOutputRegister;
}

/// <summary>
/// Closes this instance.
/// </summary>
void HashJoinOperator::Close()
{
	mInputRight.Close();
	for ( uint32_t i = 0; i < mInputRegisterLeft.size(); ++i )
	{
		SDELETE( mOutputRegister[i] );
	}
}
