#include "elog/logger.h"
#include "koro/timestamp.h"
#include <cassert>

using namespace koro;

int main()
{
	Timestamp t1 = Timestamp::now();
	std::cout << "Timestamp size: " << sizeof(t1) << std::endl;
	std::cout << t1.toItimerspec().it_value.tv_sec << ":"
			  << t1.toItimerspec().it_value.tv_nsec << std::endl;
	Timestamp t2 = t1 + 2.5;
	std::cout << t2.toItimerspec().it_value.tv_sec << ":"
			  << t2.toItimerspec().it_value.tv_nsec << std::endl;

	assert(t1 == t1);
	assert(t1 < t2);
	assert(t1 <= t2);
	assert(t1 != t2);
}