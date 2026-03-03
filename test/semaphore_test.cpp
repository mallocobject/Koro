#include "koro/semaphore.h"
#include <iostream>

using namespace koro;

int main()
{
	Semaphore signal(1);

	signal.wait();
	std::cout << "do" << std::endl;
	signal.post();
}