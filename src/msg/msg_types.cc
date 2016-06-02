
#include "msg_types.h"

#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include "common/Formatter.h"

void entity_name_t::dump(Formatter *f) const
{
  f->dump_string("type", type_str());
  f->dump_unsigned("num", num());
}

void entity_addr_t::dump(Formatter *f) const
{
  f->dump_unsigned("nonce", nonce);
  f->dump_stream("addr") << get_sockaddr();
}

void entity_inst_t::dump(Formatter *f) const
{
  f->dump_object("name", name);
  f->dump_object("addr", addr);
}

void entity_name_t::generate_test_instances(list<entity_name_t*>& o)
{
  o.push_back(new entity_name_t(entity_name_t::MON()));
  o.push_back(new entity_name_t(entity_name_t::MON(1)));
  o.push_back(new entity_name_t(entity_name_t::OSD(1)));
  o.push_back(new entity_name_t(entity_name_t::CLIENT(1)));
}

void entity_addr_t::generate_test_instances(list<entity_addr_t*>& o)
{
  o.push_back(new entity_addr_t());
  entity_addr_t *a = new entity_addr_t();
  a->set_nonce(1);
  o.push_back(a);
  entity_addr_t *b = new entity_addr_t();
  b->set_nonce(5);
  b->set_family(AF_INET);
  b->set_in4_quad(0, 127);
  b->set_in4_quad(1, 0);
  b->set_in4_quad(2, 1);
  b->set_in4_quad(3, 2);
  b->set_port(2);
  o.push_back(b);
}

void entity_inst_t::generate_test_instances(list<entity_inst_t*>& o)
{
  o.push_back(new entity_inst_t());
  entity_name_t name;
  entity_addr_t addr;
  entity_inst_t *a = new entity_inst_t(name, addr);
  o.push_back(a);
}

bool entity_addr_t::parse(const char *s, const char **end)
{
  memset(this, 0, sizeof(*this));

  const char *start = s;
  bool brackets = false;
  if (*start == '[') {
    start++;
    brackets = true;
  }
  
  // inet_pton() requires a null terminated input, so let's fill two
  // buffers, one with ipv4 allowed characters, and one with ipv6, and
  // then see which parses.
  char buf4[39];
  char *o = buf4;
  const char *p = start;
  while (o < buf4 + sizeof(buf4) &&
	 *p && ((*p == '.') ||
		(*p >= '0' && *p <= '9'))) {
    *o++ = *p++;
  }
  *o = 0;

  char buf6[64];  // actually 39 + null is sufficient.
  o = buf6;
  p = start;
  while (o < buf6 + sizeof(buf6) &&
	 *p && ((*p == ':') ||
		(*p >= '0' && *p <= '9') ||
		(*p >= 'a' && *p <= 'f') ||
		(*p >= 'A' && *p <= 'F'))) {
    *o++ = *p++;
  }
  *o = 0;
  //cout << "buf4 is '" << buf4 << "', buf6 is '" << buf6 << "'" << std::endl;

  // ipv4?
  struct in_addr a4;
  struct in6_addr a6;
  if (inet_pton(AF_INET, buf4, &a4)) {
    u.sin.sin_addr.s_addr = a4.s_addr;
    u.sa.sa_family = AF_INET;
    p = start + strlen(buf4);
  } else if (inet_pton(AF_INET6, buf6, &a6)) {
    u.sa.sa_family = AF_INET6;
    memcpy(&u.sin6.sin6_addr, &a6, sizeof(a6));
    p = start + strlen(buf6);
  } else {
    return false;
  }

  if (brackets) {
    if (*p != ']')
      return false;
    p++;
  }
  
  //cout << "p is " << *p << std::endl;
  if (*p == ':') {
    // parse a port, too!
    p++;
    int port = atoi(p);
    set_port(port);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (*p == '/') {
    // parse nonce, too
    p++;
    int non = atoi(p);
    set_nonce(non);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (end)
    *end = p;

  //cout << *this << std::endl;
  return true;
}

// entity_addrvec_t

void entity_addrvec_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MSG_ADDR2) == 0) {
    // encode a single legacy entity_addr_t for unfeatured peers
    if (v.size() > 0) {
      for (vector<entity_addr_t>::const_iterator p = v.begin();
           p != v.end(); ++p) {
        if ((*p).type == (*p).TYPE_LEGACY) {
	  ::encode(*p, bl, 0);
	  return;
	}
      }
      ::encode(v[0], bl, 0);
    } else {
      ::encode(entity_addr_t(), bl, 0);
    }
    return;
  }
  ::encode((__u8)2, bl);
  ::encode(v, bl, features);
}

void entity_addrvec_t::decode(bufferlist::iterator& bl)
{
  __u8 marker;
  ::decode(marker, bl);
  if (marker == 0) {
    // legacy!
    ::decode(marker, bl);
    __u16 rest;
    ::decode(rest, bl);
    entity_addr_t addr;
    addr.type = addr.TYPE_LEGACY;
    ::decode(addr.nonce, bl);
    sockaddr_storage ss;
#if defined(__linux__) || defined(DARWIN) || defined(__FreeBSD__)
    ::decode(ss, bl);
#else
    ceph_sockaddr_storage wireaddr;
    ::memset(&wireaddr, '\0', sizeof(wireaddr));
    ::decode(wireaddr, bl);
    unsigned copysize = MIN(sizeof(wireaddr), sizeof(ss));
    ::memcpy(&ss, &wireaddr, copysize);
#endif
    addr.set_sockaddr((sockaddr*)&ss);
    v.clear();
    v.push_back(addr);
    return;
  }
  if (marker == 1) {
    entity_addr_t addr;
    DECODE_START(1, bl);
    ::decode(addr.type, bl);
    ::decode(addr.nonce, bl);
    __u32 elen;
    ::decode(elen, bl);
    if (elen) {
      bl.copy(elen, (char*)addr.get_sockaddr());
    }
    DECODE_FINISH(bl);
    v.clear();
    v.push_back(addr);
    return;
  }
  if (marker > 2)
    throw buffer::malformed_input("entity_addrvec_marker > 2");
  ::decode(v, bl);
}

void entity_addrvec_t::dump(Formatter *f) const
{
  f->open_array_section("addrvec");
  for (vector<entity_addr_t>::const_iterator p = v.begin();
       p != v.end(); ++p) {
    f->dump_object("addr", *p);
  }
  f->close_section();
}

void entity_addrvec_t::generate_test_instances(list<entity_addrvec_t*>& ls)
{
  ls.push_back(new entity_addrvec_t());
  ls.push_back(new entity_addrvec_t());
  ls.back()->v.push_back(entity_addr_t());
  ls.push_back(new entity_addrvec_t());
  ls.back()->v.push_back(entity_addr_t());
  ls.back()->v.push_back(entity_addr_t());
}

ostream& operator<<(ostream& out, const sockaddr_storage &ss)
{
  char buf[NI_MAXHOST] = { 0 };
  char serv[NI_MAXSERV] = { 0 };
  size_t hostlen;

  if (ss.ss_family == AF_INET)
    hostlen = sizeof(struct sockaddr_in);
  else if (ss.ss_family == AF_INET6)
    hostlen = sizeof(struct sockaddr_in6);
  else
    hostlen = sizeof(struct sockaddr_storage);
  getnameinfo((struct sockaddr *)&ss, hostlen, buf, sizeof(buf),
	      serv, sizeof(serv),
	      NI_NUMERICHOST | NI_NUMERICSERV);
  if (ss.ss_family == AF_INET6)
    return out << '[' << buf << "]:" << serv;
  return out << buf << ':' << serv;
}

ostream& operator<<(ostream& out, const sockaddr *sa)
{
  char buf[NI_MAXHOST] = { 0 };
  char serv[NI_MAXSERV] = { 0 };
  size_t hostlen;

  if (sa->sa_family == AF_INET)
    hostlen = sizeof(struct sockaddr_in);
  else if (sa->sa_family == AF_INET6)
    hostlen = sizeof(struct sockaddr_in6);
  else
    hostlen = sizeof(struct sockaddr_storage);
  getnameinfo(sa, hostlen, buf, sizeof(buf),
	      serv, sizeof(serv),
	      NI_NUMERICHOST | NI_NUMERICSERV);
  if (sa->sa_family == AF_INET6)
    return out << '[' << buf << "]:" << serv;
  return out << buf << ':' << serv;
}
