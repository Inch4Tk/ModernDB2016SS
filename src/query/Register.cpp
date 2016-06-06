#include "Register.h"

/// <summary>
/// Gets the integer.
/// </summary>
/// <returns></returns>
Integer Register::GetInteger()
{
	if (this->type == SchemaTypes::Tag::Char)
	{
		return INT_MIN;
	}
	return intVar;
}

/// <summary>
/// Gets the string.
/// </summary>
/// <returns></returns>
std::string Register::GetString()
{
	if( this->type == SchemaTypes::Tag::Integer )
	{
		return "";
	}
	return stringVar;
}

/// <summary>
/// Gets the type.
/// </summary>
/// <returns></returns>
SchemaTypes::Tag Register::GetType()
{
	return type;
}

bool Register::operator!=( const Register& other ) const
{
	return !(*this == other);
}

bool Register::operator==( const Register& other ) const
{
	if ( this->type == other.type )
	{
		if( this->type == SchemaTypes::Tag::Char )
		{
			return this->stringVar == other.stringVar;
		}
		else if( this->type == SchemaTypes::Tag::Integer )
		{
			return this->intVar == other.intVar;
		}
	}
	return false;
}
