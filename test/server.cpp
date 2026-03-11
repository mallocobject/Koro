#include "elog/logger.h"
#include "koro/hook.h"
#include "koro/io_manager.h"
#include <arpa/inet.h>
#include <cassert>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

using namespace koro;

void server_loop()
{
	int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = ::htons(6666);
	::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr.s_addr);

	int ret =
		::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
	::listen(listen_fd, SOMAXCONN);

	LOG_INFO << "Server listening on 127.0.0.1:6666";

	while (true)
	{
		int peer_fd = ::accept(listen_fd, nullptr, nullptr);
		if (peer_fd < 0)
		{
			LOG_ERROR << "accept failed";
			continue;
		}

		LOG_INFO << "New client connected! fd: " << peer_fd;

		iom.submit(
			[peer_fd]()
			{
				char buf[1024];
				while (true)
				{
					ssize_t n = read(peer_fd, buf, sizeof(buf));

					if (n > 0)
					{
						std::string str(buf, n);
						LOG_INFO << "client(fd:" << peer_fd << ") -> " << str;

						write(peer_fd, buf, n);

						if (str == "[^" || str == "quit")
						{
							LOG_INFO << "client quit actively.";
							break; // 退出死循环
						}
					}
					else if (n == 0)
					{
						LOG_INFO << "client(fd:" << peer_fd
								 << ") disconnected.";
						break; // 客户端断开，退出死循环
					}
					else
					{
						LOG_ERROR << "read error on fd: " << peer_fd;
						break; // 发生错误，退出死循环
					}
				}

				close(peer_fd);
			});
	}
}

int main()
{
	iom.submit(server_loop);

	while (true)
	{
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	iom.stop();
	return 0;
}