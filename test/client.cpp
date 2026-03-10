#include "elog/logger.h"
#include "koro/hook.h"
#include <arpa/inet.h>
#include <iostream>
#include <sys/socket.h>

int main()
{
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = ::htons(6666);
	::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr.s_addr);

	int ret = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

	auto read_task = [fd]
	{
		char buf[1024];
		ssize_t n = read(fd, buf, sizeof(buf));
		if (n > 0)
		{
			std::string str(buf, n);
			LOG_INFO << "client -> " << std::string(buf, n);
		}
		else if (n == 0)
		{
			::close(fd);
		}
	};

	while (true)
	{
		iom->submit(read_task);
		std::string buf;
		std::cout << "-> ";
		std::getline(std::cin, buf);
		ssize_t n = write(fd, buf.data(), buf.size());
	}

	iom->stop();
}