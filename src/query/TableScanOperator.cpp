#include "TableScanOperator.h"

#include "Register.h"
#include "DBCore.h"
#include "buffer/BufferManager.h"

TableScanOperator::TableScanOperator( DBCore& core, BufferManager& bm ) : mCore(core), mBufferManager(bm)
{

}

TableScanOperator::~TableScanOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void TableScanOperator::Open()
{

}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool TableScanOperator::Next()
{
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> TableScanOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void TableScanOperator::Close()
{

}
