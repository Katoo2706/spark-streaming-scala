## Netcat (nc) Command Usage

Netcat or NC is a utility tool that uses TCP and UDP connections to read and write in a network. It can be used for both attacking and security. In the case of attacking. It helps us to debug the network along with investigating it. It runs on all operating systems. 

[Reference](https://phoenixnap.com/kb/nc-command)

-l (--listen): Listens for connections instead of using connect mode.

-k (--keep-open): Keeps the connection open for multiple simultaneous connections.

Socket là điểm cuối end-point trong liên kết truyền thông hai chiều (two-way communication) biểu diễn kết nối giữa Client – Server.


#### Create a small server to input data as string, this data will be sent through a socket => listen on 12345.

```bash
# In mac os
nc -l localhost -p 12345
```

Then use Spark streaming to read data from that socket.
