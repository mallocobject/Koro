#include "koro/singleton.h"
#include <iostream>

using namespace koro;

class Derive : public Singleton<Derive>
{
	friend class Singleton<Derive>;

  private:
	Derive()
	{
		std::cout << "Derive" << std::endl;
	}

  public:
	void func()
	{
		std::cout << "func" << std::endl;
	}
};

int main()
{
	Derive::instance().func();
}