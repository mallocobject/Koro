#include <iostream>
#include <sys/ucontext.h>
#include <ucontext.h>
#include <unistd.h>

void func(int a)
{
	std::cout << "child: " << a << std::endl;
}

int main()
{
	char stack[128 * 1024];

	ucontext_t child;
	ucontext_t main;

	getcontext(&child);
	child.uc_link = &main;
	child.uc_stack.ss_flags = 0;
	child.uc_stack.ss_size = sizeof(stack);
	child.uc_stack.ss_sp = stack;

	makecontext(&child, reinterpret_cast<void (*)()>(func), 1, 0);
	swapcontext(&main, &child);

	std::cout << "main" << std::endl;

	return 0;
}