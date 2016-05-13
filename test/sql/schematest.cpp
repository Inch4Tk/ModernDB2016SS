#include "sql/Schema.h"
#include "sql/SchemaParser.h"
#include "DBCore.h"
#include "utility/macros.h"

#include "gtest/gtest.h"

// Test that our schemas are the same after parsing/serialization etc etc.
class SchemaTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		core = new DBCore();
		core->WipeDatabase();
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
};

TEST_F( SchemaTest, SchemaEquality )
{
	// Tests if the same two schemas are equal and different schemas are not
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql( sql, true );
	std::unique_ptr<Schema> sSql = parserSql.parse();

	std::string sql2 = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql2( sql2, true );
	std::unique_ptr<Schema> sSql2 = parserSql2.parse();

	EXPECT_EQ( *sSql, *sSql2 );

	// changed the typo in salary back as a change xD
	std::string sql3 = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salery integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql3( sql3, true );
	std::unique_ptr<Schema> sSql3 = parserSql3.parse();
	EXPECT_NE( *sSql, *sSql3 );
}

TEST_F(SchemaTest, ParseFileStringEq)
{
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql( sql, true );
	std::unique_ptr<Schema> sSql = parserSql.parse();

	std::string filepath = "testSchema.sql";
	SchemaParser parserFile( filepath, false );
	std::unique_ptr<Schema> sFile = parserFile.parse();

	EXPECT_EQ( *sSql, *sFile );
}

