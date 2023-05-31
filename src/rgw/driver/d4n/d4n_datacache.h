#ifndef CEPH_D4NCACHE_H
#define CEPH_D4NCACHE_H

#include "rgw_common.h"
#include "d4n_directory.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

namespace rgw { namespace d4n {

class D4NDatacache {
  public:
    CephContext* cct;

    D4NDatacache() {}
    D4NDatacache(std::string host, int port) {
      addr.host = host;
      addr.port = port;
    }

    void init(CephContext* _cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }

    int find_client(cpp_redis::client *client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }

    int copy_data(std::string originalOid, std::string copyOid);
    int append_data(std::string oid, buffer::list& data); // done, need get (or iterate) -Sam
    int del_data(std::string oid);

    int set_attrs(std::string oid, rgw::sal::Attrs* attrs);
    int get_attrs(std::string oid, rgw::sal::Attrs* newAttrs, std::vector< std::pair<std::string, std::string> >* newMetadata);
    int copy_attrs(std::string originalOid, std::string copyOid, rgw::sal::Attrs* attrs);
    int update_attr(std::string oid, rgw::sal::Attrs* attr);
    int del_attrs(std::string oid, std::vector<std::string>& baseFields, std::vector<std::string>& deleteFields);

    int del_object(std::string oid);

  private:
    cpp_redis::client client;
    Address addr;
    std::vector< std::pair<std::string, std::string> > build_data(bufferlist&& data);
    std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary);
};

} } // namespace rgw::d4n

#endif
