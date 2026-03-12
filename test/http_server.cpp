#include "elog/logger.h"
#include "koro/hook.h"
#include "koro/io_manager.h"
#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <cassert>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
using namespace koro;

int main()
{
	int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
	int reuse = 1;
	::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = ::htons(8080);
	::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr.s_addr);

	int ret =
		::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
	::listen(listen_fd, SOMAXCONN);

	auto task = [listen_fd]
	{
		while (true)
		{
			int client_fd = ::accept(listen_fd, nullptr, nullptr);
			if (client_fd > 0)
			{
				auto child_task = [client_fd]
				{
					char buf[1024];
					int n = read(client_fd, buf, sizeof(buf));
					if (n > 0)
					{
						// LOG_INFO << "Receive new HTTP Request: "
						// 		 << std::string(buf, n);

						const char* response =
							"HTTP/1.1 200 OK\r\n"
							"Content-Type: text/html; charset=utf-8\r\n"
							"Content-Length: 46\r\n"
							"Connection: close\r\n\r\n"
							"<html><body><h1>Hello Koro!</h1></body></html>";

						write(client_fd, response, strlen(response));
					}
					close(client_fd);
				};
				iom.submit(child_task);
			}
		}
	};
	iom.submit(task);

	LOG_INFO << "HTTP Server is running on http://127.0.0.1:8080";

	iom.stop();
}