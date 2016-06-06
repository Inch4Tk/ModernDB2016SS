#pragma once
#ifndef REGISTER_H
#define REGISTER_H

#include "sql/SchemaTypes.h"

class Register
{
public:
	Integer GetInteger();
	std::string GetString();

	SchemaTypes::Tag GetType();

	bool operator==( const Register& other ) const;
	bool operator!=( const Register& other ) const;

	// No need for hash since using unordered map later
private:
	Integer intVar;
	std::string stringVar;
	SchemaTypes::Tag type;
};
#endif