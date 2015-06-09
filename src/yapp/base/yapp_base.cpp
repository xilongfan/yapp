#include "./yapp_base.h"

using namespace yapp::base;

ZkServer::ZkServer(const string & host, const string & port) {
  host_str = host;
  port_str = port;
}

ZkServer::~ZkServer() {}

string ZkServer::get_connection_str() {
  return (host_str + ZK_SRV_HOST_PORT_DELIM + port_str);
}
