#include "DBCore.h"
#include "buffer/SPSegment.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "gtest/gtest.h"

#include <vector>
#include <unordered_map>


const std::vector<std::string> testData = {
	"640K ought to be enough for anybody",
	"Beware of bugs in the above code; I have only proved it correct, not tried it",
	"Tape is Dead. Disk is Tape. Flash is Disk.",
	"for seminal contributions to database and transaction processing research and technical leadership in system implementation",
	"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce iaculis risus ut ipsum pellentesque vitae venenatis elit viverra. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Curabitur ante mi, auctor in aliquet non, sagittis ac est. Phasellus in viverra mauris. Quisque scelerisque nisl eget sapien venenatis nec consectetur odio aliquam. Maecenas lobortis mattis semper. Ut lacinia urna nec lorem lacinia consectetur. In non enim vitae dui rhoncus dictum. Sed vel fringilla felis. Curabitur tincidunt justo ac nulla scelerisque accumsan. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Cras tempor venenatis orci, quis vulputate risus dapibus id. Aliquam elementum congue nulla, eget tempus justo fringilla sed. Maecenas odio erat, commodo a blandit quis, tincidunt vel risus. Proin sed ornare tellus. Donec tincidunt urna ac turpis rutrum varius. Etiam vehicula semper velit ut mollis. Aliquam quis sem massa. Morbi ut massa quis purus ullamcorper aliquet. Sed nisi justo, fermentum id placerat eu, dignissim eu elit. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse interdum laoreet commodo. Nullam turpis velit, tristique in sodales sit amet, molestie et diam. Quisque blandit velit quis augue sodales vestibulum. Phasellus ut magna non arcu egestas volutpat. Etiam id ultricies ligula. Donec non lectus eget risus lobortis pretium. Sed rutrum augue eu tellus scelerisque sit amet interdum massa volutpat. Maecenas nunc ligula, blandit quis adipiscing eget, fermentum nec massa. Vivamus in commodo nunc. Quisque elit mi, consequat eget vestibulum lacinia, ultrices eu purus. Vestibulum tincidunt consequat nulla, quis tempus eros volutpat sed. Aliquam elementum massa vel ligula bibendum aliquet non nec purus. Nunc sollicitudin orci sed nisi eleifend molestie. Praesent scelerisque vehicula quam et dignissim. Suspendisse potenti. Sed lacus est, aliquet auctor mollis ac, iaculis at metus. Aenean at risus sed lectus volutpat bibendum non id odio. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Mauris purus lorem, congue ac tristique sit amet, gravida eu neque. Nullam lacus tellus, venenatis a blandit ac, consequat sed massa. Mauris ultrices laoreet lorem. Nam elementum, est vel elementum commodo, enim tellus mattis diam, a bibendum mi enim vitae magna. Aliquam nisi dolor, aliquam at porta sit amet, tristique id nulla. In purus leo, tristique eget faucibus id, pharetra vel diam. Nunc eleifend commodo feugiat. Mauris sed diam quis est dictum rutrum in eu erat. Suspendisse potenti. Duis adipiscing nisl eu augue dignissim sagittis. Praesent vitae nisl dolor. Duis interdum, dolor a viverra imperdiet, lorem lectus luctus sem, sit amet rutrum augue dolor id erat. Vestibulum ac orci condimentum velit mollis scelerisque eu eu est. Aenean fringilla placerat enim, placerat adipiscing felis feugiat quis. Cras sed."
};

// Test that our segments
class SegmentTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		srand( static_cast<uint32_t>(time( NULL )) );
		core = new DBCore();
		core->WipeDatabase();
		std::string sql = "create table dbtest ( strentry char(5000), primary key (strentry));";
		core->AddRelationsFromString( sql );
		segment = core->GetSPSegment( "dbtest" );
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
	std::unique_ptr<SPSegment> segment;
};


TEST_F(SegmentTest, InsertLookup )
{
	std::unordered_map<TID, uint32_t> values; // TID -> testData entry

	// Insert some records
	for ( uint32_t i = 0; i < 500; ++i )
	{
		// Select string/record to insert
		uint64_t r = rand() % testData.size();
		const std::string s = testData[r];

		// Insert record
		TID tid = segment->Insert( Record( static_cast<uint32_t>(s.size()),
									  reinterpret_cast<const uint8_t*>(s.c_str()) ) );
		EXPECT_EQ( values.find( tid ), values.end() ); // TIDs should not be overwritten
		values[tid] = static_cast<uint32_t>(r);
	}
	// Every value has to be the same as it was inserted
	for ( std::pair<TID, uint32_t> tidValue : values )
	{
		const std::string& value = testData[tidValue.second];
		uint32_t len = static_cast<uint32_t>(value.size());
		Record rec = segment->Lookup( tidValue.first );
		EXPECT_EQ( len, rec.GetLen() );
		EXPECT_EQ( 0, memcmp( rec.GetData(), value.c_str(), len ) );
	}
};

TEST_F( SegmentTest, Remove )
{
	std::unordered_map<TID, uint32_t> values; // TID -> testData entry
	// Insert some records
	for ( uint32_t i = 0; i < 500; ++i )
	{
		// Select string/record to insert
		uint64_t r = rand() % testData.size();
		const std::string s = testData[r];

		// Insert record
		TID tid = segment->Insert( Record( static_cast<uint32_t>(s.size()),
										   reinterpret_cast<const uint8_t*>(s.c_str()) ) );
		EXPECT_EQ( values.find( tid ), values.end() ); // TIDs should not be overwritten
		values[tid] = static_cast<uint32_t>(r);
	}
	std::unordered_map<TID, bool> removed; // TID -> is removed
	
	// Remove records
	for ( std::pair<TID, uint32_t> tidValue : values )
	{
		if ( rand() > RAND_MAX * 0.5 )
		{
			bool removeSuccess = segment->Remove( tidValue.first );
			EXPECT_EQ( true, removeSuccess );
			removed.insert( std::make_pair( tidValue.first, true ) );
		}
		else
		{
			removed.insert( std::make_pair( tidValue.first, false ) );
		}
	}

	// Check over every record if it was correctly removed/kept
	for ( std::pair<TID, uint32_t> tidValue : values )
	{
		if ( removed[tidValue.first] )
		{
			Record rec = segment->Lookup( tidValue.first );
			EXPECT_EQ( 0, rec.GetLen() );
			EXPECT_EQ( nullptr, rec.GetData() );
		}
		else
		{
			const std::string& value = testData[tidValue.second];
			uint32_t len = static_cast<uint32_t>(value.size());
			Record rec = segment->Lookup( tidValue.first );
			EXPECT_EQ( len, rec.GetLen() );
			EXPECT_EQ( 0, memcmp( rec.GetData(), value.c_str(), len ) );
		}
	}
};

TEST_F( SegmentTest, Update )
{
	std::unordered_map<TID, uint32_t> values; // TID -> testData entry
	std::vector<TID> tids;

	// Insert some records
	for ( uint32_t i = 0; i < 500; ++i )
	{
		// Select string/record to insert
		uint64_t r = rand() % testData.size();
		const std::string s = testData[r];

		// Insert record
		TID tid = segment->Insert( Record( static_cast<uint32_t>(s.size()),
										   reinterpret_cast<const uint8_t*>(s.c_str()) ) );
		tids.push_back( tid );
		EXPECT_EQ( values.find( tid ), values.end() ); // TIDs should not be overwritten
		values[tid] = static_cast<uint32_t>(r);
	}
	
	// Update random records
	for ( uint32_t i = 0; i < 1000; ++i )
	{
		// Select new string / record
		TID target = tids[rand() % tids.size()];
		uint32_t r = rand() % testData.size();
		const std::string s = testData[r];

		// Replace old with new value
		bool success = segment->Update( target, Record( static_cast<uint32_t>(s.size()),
								 reinterpret_cast<const uint8_t*>(s.c_str()) ) );
		EXPECT_EQ( true, success );
		values[target] = static_cast<uint32_t>(r);
	}

	// Every value has to be the same as it was inserted/updated to
	for ( std::pair<TID, uint32_t> tidValue : values )
	{
		const std::string& value = testData[tidValue.second];
		uint32_t len = static_cast<uint32_t>(value.size());
		Record rec = segment->Lookup( tidValue.first );
		EXPECT_EQ( len, rec.GetLen() );
		EXPECT_EQ( 0, memcmp( rec.GetData(), value.c_str(), len ) );
	}
};

TEST_F( SegmentTest, RandomOperations )
{
	// Random interspersed operations
	std::unordered_map<TID, uint32_t> values; // TID -> testData entry
	std::vector<TID> tids;

	// Insert some baseline records
	for ( uint32_t i = 0; i < 100; ++i )
	{
		// Select string/record to insert
		uint64_t r = rand() % testData.size();
		const std::string s = testData[r];

		// Insert record
		TID tid = segment->Insert( Record( static_cast<uint32_t>(s.size()),
										   reinterpret_cast<const uint8_t*>(s.c_str()) ) );
		tids.push_back( tid );
		EXPECT_EQ( values.find( tid ), values.end() ); // TIDs should not be overwritten
		values[tid] = static_cast<uint32_t>(r);
	}

	// Random ops
	for ( uint32_t i = 0; i < 10000; ++i )
	{
		float chance = static_cast<float>(rand()) / RAND_MAX;
		if (chance < 0.70)
		{
			// Insert
			// Select string/record to insert
			uint64_t r = rand() % testData.size();
			const std::string s = testData[r];

			// Insert record
			TID tid = segment->Insert( Record( static_cast<uint32_t>(s.size()),
											   reinterpret_cast<const uint8_t*>(s.c_str()) ) );

			EXPECT_EQ( values.find( tid ), values.end() ); // TIDs should not be overwritten
			tids.push_back( tid );
			values[tid] = static_cast<uint32_t>(r);
		}
		else if (chance < 0.9)
		{
			// Update
			// Select new string / record
			TID target = tids[rand() % tids.size()];
			uint32_t r = rand() % testData.size();
			const std::string s = testData[r];

			// Replace old with new value
			bool success = segment->Update( target, Record( static_cast<uint32_t>(s.size()),
															reinterpret_cast<const uint8_t*>(s.c_str()) ) );

			EXPECT_EQ( true, success );
			values[target] = static_cast<uint32_t>(r);
		}
		else
		{
			// Remove
			uint32_t inTid = rand() % tids.size();
			TID target = tids[inTid];
			bool removeSuccess = segment->Remove( target );
			EXPECT_EQ( true, removeSuccess );
			// Remove from our tid map and tid vector as well
			values.erase( target );
			tids[inTid] = tids[tids.size() - 1];
			tids.pop_back();
		}
	}

	// Every value has to be the same as it was inserted/updated to
	for ( std::pair<TID, uint32_t> tidValue : values )
	{
		// Not removed, normal check
		const std::string& value = testData[tidValue.second];
		uint32_t len = static_cast<uint32_t>(value.size());
		Record rec = segment->Lookup( tidValue.first );
		EXPECT_EQ( len, rec.GetLen() );
		EXPECT_EQ( 0, memcmp( rec.GetData(), value.c_str(), len ) );
	}
};