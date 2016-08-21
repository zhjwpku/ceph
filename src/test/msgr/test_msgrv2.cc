// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "msg/Dispatcher.h"
#include "msg/msg_types.h"
#include "msg/Message.h"
#include "msg/Messenger.h"
#include "msg/Connection.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "include/buffer.h"
#include "auth/AuthSessionHandler.h"

#include <gtest/gtest.h>

#define CEPH_MSGR2_BANNER_LEN 38

static const int ASYNC_IOV_MAX = (IOV_MAX >= 1024 ? IOV_MAX / 4 : IOV_MAX);
struct iovec msgvec[ASYNC_IOV_MAX];
bufferlist outcoming_bl;
char *state_buffer;
uint64_t state_offset;
char *recv_buf;
// TCP_PREFETC_MIN_SIZE
uint32_t recv_max_prefetch = 512;
uint32_t recv_start;
uint32_t recv_end;
Mutex write_lock("write_lock");
ceph_msg_connect connect_msg;
ceph_msg_connect_reply connect_reply;

static int set_nonblock(int sd)
{
  int flags;

  if ((flags = fcntl(sd, F_GETFL)) < 0) {
    cerr << __func__ << " fcntl(F_GETFL) failed: " << strerror(errno) << std::endl; 
    return -errno;
  }
  if (fcntl(sd, F_SETFL, flags | O_NONBLOCK) < 0) {
    cerr << __func__ << " fcntl(F_SETFL, O_NONBLOCK): " << strerror(errno) << std::endl;
    return -errno;
  }

  return 0;
}

static void set_socket_options(int sd)
{
  // omitted cct->_conf->ms_tcp_nodelay and ms_tcp_rcvbuf

  // block ESIGPIPE
#ifdef SO_NOSIGPIPE
  int val = 1;
  int r = ::setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (void*)&val, sizeof(val));
  if (r) {
    r = -errno;
    cerr << __func__ << "couldn't set SO_NOSIGPIPE: " << cpp_strerror(r) << std::endl;
  }
#endif
}

static int generic_connect(const entity_addr_t& addr)
{
  int ret;

  int s = ::socket(addr.get_family(), SOCK_STREAM, 0);
  if (s < 0)
    return s;

  ret = set_nonblock(s);
  if (ret < 0) {
    close(s);
    return ret;
  }

  set_socket_options(s);

  ret = ::connect(s, addr.get_sockaddr(), addr.get_sockaddr_len());
  if (ret < 0) {
    if (errno == EINPROGRESS)
      return s;

    cout << __func__ << " connect:" << strerror(errno) << std::endl;
    close(s);
    return -errno;
  }

  return s;
}

static int reconnect(const entity_addr_t &addr, int sd)
{
  int ret = ::connect(sd, addr.get_sockaddr(), addr.get_sockaddr_len());

  if (ret < 0 && errno != EISCONN) {
    cout << __func__ << " reconnect: " << strerror(errno) << std::endl;
    if (errno == EINPROGRESS || errno == EALREADY)
      return 1;
    return -errno;
  }

  return 0;
}

static int _bind(const entity_addr_t &bind_addr, entity_addr_t &listen_addr, int &listen_sd)
{
  int family = bind_addr.get_family();

  /* socket creation */
  listen_sd = ::socket(family, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    cerr << __func__ << " unable to create socket: "
        << cpp_strerror(errno) << std::endl;
    return -errno;
  }

  int r = set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    listen_sd = -1;
    return r;
  }

  set_socket_options(listen_sd);

  // use whatever user specified (if anything)
  listen_addr = bind_addr;
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  r = -1;

  for (int i = 0; i < 5; i++) {
    if (i > 0) {
      cerr << __func__ << " was unable to bind. Trying again in "
                       << " 3 seconds " << std::endl;
      sleep(3);
    }

    if (listen_addr.get_port()) {
      // specific port
      // reuse addr+port when possible
      int on = 1;
      rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
      if (rc < 0) {
        cerr << __func__ << " unable to setsockopt: " << cpp_strerror(errno) << std::endl;
        r = -errno;
        continue;
      }

      rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
		  listen_addr.get_sockaddr_len());
      if (rc < 0) {
        cerr << __func__ << " unable to bind to " << listen_addr
                         << ": " << cpp_strerror(errno) << std::endl;
        r = -errno;
        continue;
      }
    } else {
      // try a range of ports
      for (int port = 16801; port <= 16900; port++) {

        listen_addr.set_port(port);
        rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
		    listen_addr.get_sockaddr_len());
        if (rc == 0)
          break;
      }
      if (rc < 0) {
        cerr << __func__ << " unable to bind to " << listen_addr
                         << " on any port in range " << "16801"
                         << "-" << "16900" << ": "
                         << cpp_strerror(errno) << std::endl;
        r = -errno;
        listen_addr.set_port(0); // Clear port before retry, otherwise we shall fail again.
        continue;
      }
      cout << __func__ << " bound on random port " << listen_addr << std::endl;
    }
    if (rc == 0)
      break;
  }
  // It seems that binding completely failed, return with that exit status
  if (rc < 0) {
    cerr << __func__ << " was unable to bind after " << "5"
                     << " attempts: " << cpp_strerror(errno) << std::endl;
    ::close(listen_sd);
    listen_sd = -1;
    return r;
  }

  // what port did we get?
  sockaddr_storage ss;
  socklen_t llen = sizeof(ss);
  rc = getsockname(listen_sd, (sockaddr*)&ss, &llen);
  if (rc < 0) {
    rc = -errno;
    cerr << __func__ << " failed getsockname: " << cpp_strerror(rc) << std::endl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }
  listen_addr.set_sockaddr((sockaddr*)&ss);

  cout << __func__ << " bound to " << listen_addr << std::endl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    rc = -errno;
    cerr << __func__ << " unable to listen on " << listen_addr
        << ": " << cpp_strerror(rc) << std::endl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }

  return 0;
}

static int _accept(int listen_sd)
{
  int errors = 0;
  while (errors < 4) {
    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int sd = ::accept(listen_sd, (sockaddr *)&ss, &slen);
    if (sd >= 0) {
      cout << __func__ << " accepted incoming on sd " << sd << std::endl;
      return sd;
    } else {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN) {
        break;
      } else {
        errors++;
        cout << __func__ << " no incoming connection? sd = " << sd
             << " errno " << errno << " " << cpp_strerror(errno) << std::endl;
      }
    }
  }
  return -1;
}

static void suppress_sigpipe()
{
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
  /*
    We want to ignore possible SIGPIPE that we can generate on write.
    SIGPIPE is delivered *synchronously* and *only* to the thread
    doing the write.  So if it is reported as already pending (which
    means the thread blocks it), then we do nothing: if we generate
    SIGPIPE, it will be merged with the pending one (there's no
    queuing), and that suits us well.  If it is not pending, we block
    it in this thread (and we avoid changing signal action, because it
    is per-process).
  */
  sigset_t pending;
  sigemptyset(&pending);
  sigpending(&pending);
  sigpipe_pending = sigismember(&pending, SIGPIPE);
  if (!sigpipe_pending) {
    sigset_t blocked;
    sigemptyset(&blocked);
    pthread_sigmask(SIG_BLOCK, &sigpipe_mask, &blocked);

    /* Maybe is was blocked already?  */
    sigpipe_unblock = ! sigismember(&blocked, SIGPIPE);
  }
#endif /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
}

static void restore_sigpipe()
{
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
  /*
    If SIGPIPE was pending already we do nothing.  Otherwise, if it
    become pending (i.e., we generated it), then we sigwait() it (thus
    clearing pending status).  Then we unblock SIGPIPE, but only if it
    were us who blocked it.
  */
  if (!sigpipe_pending) {
    sigset_t pending;
    sigemptyset(&pending);
    sigpending(&pending);
    if (sigismember(&pending, SIGPIPE)) {
      /*
        Protect ourselves from a situation when SIGPIPE was sent
        by the user to the whole process, and was delivered to
        other thread before we had a chance to wait for it.
      */
      static const struct timespec nowait = { 0, 0 };
      TEMP_FAILURE_RETRY(sigtimedwait(&sigpipe_mask, NULL, &nowait));
    }

    if (sigpipe_unblock)
      pthread_sigmask(SIG_UNBLOCK, &sigpipe_mask, NULL);
  }
#endif  /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
}

// return the length of msg needed to be sent,
// < 0 means error occured
static ssize_t do_sendmsg(int sd, struct msghdr &msg, unsigned len, bool more)
{
  suppress_sigpipe();

  while (len > 0) {
    ssize_t r;
#if defined(MSG_NOSIGNAL)
    r = ::sendmsg(sd, &msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
#else
    r = ::sendmsg(sd, &msg, (more ? MSG_MORE : 0));
#endif /* defined(MSG_NOSIGNAL) */

    if (r == 0) {
      cout << __func__ << " sendmsg got r==0!" << std::endl;
    } else if (r < 0) {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN) {
        break;
      } else {
        cerr << __func__ << " sendmsg error: " << cpp_strerror(errno) << std::endl;
        restore_sigpipe();
        return r;
      }
    }

    len -= r;
    if (len == 0) break;

    // hrmph. drain r bytes from the front of our message.
    while (r > 0) {
      if (msg.msg_iov[0].iov_len <= (size_t)r) {
        // drain this whole item
        r -= msg.msg_iov[0].iov_len;
        msg.msg_iov++;
        msg.msg_iovlen--;
      } else {
        msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + r;
        msg.msg_iov[0].iov_len -= r;
        break;
      }
    }
  }
  restore_sigpipe();
  return (ssize_t)len;
}

static ssize_t _try_send(int sd, bool more)
{
  uint64_t sent_bytes = 0;
  list<bufferptr>::const_iterator pb = outcoming_bl.buffers().begin();
  uint64_t left_pbrs = outcoming_bl.buffers().size();
  while (left_pbrs) {
    struct msghdr msg;
    uint64_t size = MIN(left_pbrs, ASYNC_IOV_MAX);
    left_pbrs -= size;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iovlen = 0;
    msg.msg_iov = msgvec;
    unsigned msglen = 0;
    while (size > 0) {
      msgvec[msg.msg_iovlen].iov_base = (void*)(pb->c_str());
      msgvec[msg.msg_iovlen].iov_len = pb->length();
      msg.msg_iovlen++;
      msglen += pb->length();
      ++pb;
      size--;
    }

    ssize_t r = do_sendmsg(sd, msg, msglen, left_pbrs || more);
    if (r < 0)
      return r;

    // "r" is the remaining length
    sent_bytes += msglen - r;
    if (r > 0) {
      cout << __func__ << " remaining " << r
                          << " needed to be sent, creating event for writing"
                          << std::endl;
      break;
    }
    // only "r" == 0 continue
  }

  // trim already sent for outcoming_bl
  if (sent_bytes) {
    if (sent_bytes < outcoming_bl.length()) {
      outcoming_bl.splice(0, sent_bytes);
    } else {
      outcoming_bl.clear();
    }
  }

  cout << __func__ << " sent bytes " << sent_bytes
                             << " remaining bytes " << outcoming_bl.length() << std::endl;

  return outcoming_bl.length();
}

static ssize_t try_send(int sd, bufferlist &bl, bool more=false)
{
  Mutex::Locker l(write_lock);
  outcoming_bl.claim_append(bl);
  return _try_send(sd, more);
}

static ssize_t read_bulk(int fd, char *buf, unsigned len)
{
  ssize_t nread;
 again:
  nread = ::read(fd, buf, len);
  if (nread == -1) {
    if (errno == EAGAIN) {
      nread = 0;
    } else if (errno == EINTR) {
      goto again;
    } else {
      cerr << __func__ << " reading from fd=" << fd
                          << " : "<< strerror(errno) << std::endl;
      return -1;
    }
  } else if (nread == 0) {
    cerr << __func__ << " peer close file descriptor "
                              << fd << std::endl;
    return -1;
  }
  return nread;
}

static ssize_t read_until(int sd, unsigned len, char *p)
{
  ssize_t r = 0;
  uint64_t left = len - state_offset;
  if (recv_end > recv_start) {
    uint64_t to_read = MIN(recv_end - recv_start, left);
    memcpy(p, recv_buf+recv_start, to_read);
    recv_start += to_read;
    left -= to_read;
    cout << __func__ << " got " << to_read << " in buffer "
                               << " left is " << left << " buffer still has "
                               << recv_end - recv_start << std::endl;
    if (left == 0) {
      return 0;
    }
    state_offset += to_read;
  }

  recv_end = recv_start = 0;
  // nothing left in the prefetch buffer
  if (len > recv_max_prefetch) {
    // this was a large read, we don't prefetch for these 
    do {
      r = read_bulk(sd, p+state_offset, left);
      cout << __func__ << " read_bulk left is " << left << " got " << r << std::endl;
      if (r < 0) {
        cerr << __func__ << " read failed" << std::endl;
        return -1;
      } else if (r == static_cast<int>(left)) {
        state_offset = 0;
        return 0;
      }
      state_offset += r;
      left -= r;
    } while (r > 0);
  } else {
    do {
      r = read_bulk(sd, recv_buf+recv_end, recv_max_prefetch);
      cout << __func__ << " read_bulk recv_end is " << recv_end
                                 << " left is " << left << " got " << r << std::endl;
      if (r < 0) {
        cerr << __func__ << " read failed" << std::endl;
        return -1;
      }
      recv_end += r;
      if (r >= static_cast<int>(left)) {
        recv_start = len - state_offset;
        memcpy(p+state_offset, recv_buf, recv_start);
        state_offset = 0;
        return 0;
      }
      left -= r;
    } while (r > 0);
    memcpy(p+state_offset, recv_buf, recv_end-recv_start);
    state_offset += (recv_end - recv_start);
    recv_end = recv_start = 0;
  }
  cout << __func__ << " need len " << len << " remaining "
                             << len - state_offset << " bytes" << std::endl;
  return len - state_offset;
}

static ssize_t read_stream_id_and_frame_len_and_tag(int sd, __u32 &stream_id,
                                                    __u32 &frame_len, char &tag)
{
  ssize_t r = 0;
  char tmp_tag = -1;

read_stream_id:
  r = read_until(sd, sizeof(__u32), state_buffer);
  if (r < 0) {
    cerr << " __func__" << " read stream_id failed" << std::endl;
    return r;
  } else if (r > 0) {
    goto read_stream_id;
  }
  stream_id = *((__u32 *)state_buffer);

read_frame_len:
  r = read_until(sd, sizeof(__u32), state_buffer);
  if (r < 0) {
    cerr << "  __func__" << " read frame_len failed" << std::endl;
    return r;
  } else if (r > 0) {
    goto read_frame_len;
  }
  frame_len = *((__u32 *)state_buffer);

read_tag:
  r = read_until(sd, sizeof(char), &tmp_tag);
  if (r < 0) {
    cerr << "  __func__" << " read tag failed" << std::endl;
    return r;
  } else if (r > 0) {
    goto read_tag;
  }
  tag = tmp_tag;
  
  return 0;
}

#if GTEST_HAS_PARAM_TEST

class Msgrv2Test : public ::testing::TestWithParam<const char*> {
 public:
  Messenger *server_msgr;
  Messenger *client_msgr;

  Msgrv2Test(): server_msgr(NULL), client_msgr(NULL) {}
  virtual void SetUp() {
    cerr << __func__ << " start set up " << GetParam() << std::endl;
    server_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::OSD(0), "server", getpid());
    client_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::CLIENT(-1), "client", getpid());
    server_msgr->set_default_policy(Messenger::Policy::stateless_server(0, 0));
    client_msgr->set_default_policy(Messenger::Policy::lossy_client(0, 0));
  }
  virtual void TearDown() {
    ASSERT_EQ(server_msgr->get_dispatch_queue_len(), 0);
    ASSERT_EQ(client_msgr->get_dispatch_queue_len(), 0);
    delete server_msgr;
    delete client_msgr;
  }

};

class FakeDispatcher : public Dispatcher {
 public:
  struct Session : public RefCountedObject {
    Mutex lock;
    uint64_t count;
    ConnectionRef con;

    explicit Session(ConnectionRef c): RefCountedObject(g_ceph_context), lock("FakeDispatcher::Session::lock"), count(0), con(c) {
    }
    uint64_t get_count() { return count; }
  };

  Mutex lock;
  Cond cond;
  bool is_server;
  bool got_new;
  bool got_remote_reset;
  bool got_connect;
  bool loopback;

  explicit FakeDispatcher(bool s): Dispatcher(g_ceph_context), lock("FakeDispatcher::lock"),
                          is_server(s), got_new(false), got_remote_reset(false),
                          got_connect(false), loopback(false) {}
  bool ms_can_fast_dispatch_any() const { return true; }
  bool ms_can_fast_dispatch(Message *m) const {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) {
    lock.Lock();
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
      cerr << __func__ << " con: " << con << " count: " << s->count << std::endl;
    }
    s->put();
    got_connect = true;
    cond.Signal();
    lock.Unlock();
  }
  void ms_handle_fast_accept(Connection *con) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
    }
    s->put();
  }
  bool ms_dispatch(Message *m) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker l1(s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server) {
      reply_message(m);
    }
    got_new = true;
    cond.Signal();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    Mutex::Locker l(lock);
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (s) {
      s->con.reset(NULL);  // break con <-> session ref cycle
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      s->put();
    }
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {
    Mutex::Locker l(lock);
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (s) {
      s->con.reset(NULL);  // break con <-> session ref cycle
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      s->put();
    }
    got_remote_reset = true;
  }
  void ms_fast_dispatch(Message *m) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker l1(s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server) {
      if (loopback)
        assert(m->get_source().is_osd());
      else
        reply_message(m);
    } else if (loopback) {
      assert(m->get_source().is_client());
    }
    got_new = true;
    cond.Signal();
    m->put();
  }

  bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                            bufferlist& authorizer, bufferlist& authorizer_reply,
                            bool& isvalid, CryptoKey& session_key) {
    isvalid = true;
    return true;
  }

  void reply_message(Message *m) {
    MPing *rm = new MPing();
    m->get_connection()->send_message(rm);
  }
};

typedef FakeDispatcher::Session Session;

TEST_P(Msgrv2Test, FakeClientTest) {
  FakeDispatcher srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1:16800");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  // below is a fake client
  int ret;
  atomic64_t in_seq(0);
  //int global_seq(0);

  entity_inst_t inst = server_msgr->get_myinst();
  // STATE_CONNECTING
  memset(&connect_msg, 0, sizeof(connect_msg));
  memset(&connect_reply, 0, sizeof(connect_reply));
  int sd = generic_connect(inst.addr);  
  ASSERT_GT(sd, 0);
  
  // STATE_CONNECTING_RE
  ret = reconnect(inst.addr, sd);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  char msgr2_banner[CEPH_MSGR2_BANNER_LEN+1];
  __u64 protocol_features_supported = 0;
  __u64 protocol_features_required = 0;

  sprintf(msgr2_banner, "ceph %16llx %16llx", protocol_features_supported, protocol_features_required);

  bl.append(msgr2_banner, CEPH_MSGR2_BANNER_LEN);

  // always try to send if outcoming_bl has data
  while((ret = try_send(sd, bl)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY
  entity_addr_t paddr, peer_addr_for_me;
  bufferlist myaddrbl;

  unsigned need_len = CEPH_MSGR2_BANNER_LEN + sizeof(ceph_entity_addr)*2;
  while((ret = read_until(sd, need_len, state_buffer)) > 0);
  ASSERT_EQ(ret, 0);

  char recv_banner[CEPH_MSGR2_BANNER_LEN+1];
  strncpy(recv_banner, state_buffer, CEPH_MSGR2_BANNER_LEN);
  // TODO: check the recv_banner
  
  bufferlist bl1;
  bl1.append(state_buffer + CEPH_MSGR2_BANNER_LEN, sizeof(ceph_entity_addr)*2);
  bufferlist::iterator p = bl.begin();
  try {
    ::decode(paddr, p);
    ::decode(peer_addr_for_me, p);
  } catch (const buffer::error& e) {
    cerr << __func__ << " decode peer addr failed " << std::endl;
  }

  ASSERT_TRUE(paddr == inst.addr);

  bufferlist _myaddrbl;
  ::encode(peer_addr_for_me, _myaddrbl, 0);
  ret = try_send(sd, _myaddrbl);
  ASSERT_EQ(ret, 0);

  // STATE_CONNECTING_WAIT_AUTH_METHODS
  __u32 _stream_id = -1;
  __u32 _frame_len = -1;
  __u32 _auth_num = -1;
  __u32 _auth_method = -1;
  char tag = 0;
  while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(tag, CEPH_MSGR_TAG_AUTH_METHODS);

  while((ret = read_until(sd, sizeof(__u32), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);
  _auth_num = *((__u32 *)state_buffer);
  ASSERT_EQ(_frame_len, sizeof(char) + (_auth_num + 1)*sizeof(__u32));

  for (__u32 i = 0; i < _auth_num; i++) {
    while((ret = read_until(sd, sizeof(__u32), state_buffer)) > 0);
    ASSERT_EQ(ret, 0);
    _auth_method = *((__u32 *)state_buffer);
  }
  ASSERT_GT(_auth_method, 0);

  bufferlist bl2;
  bl2.append((char*)&_stream_id, sizeof(_stream_id));
  _frame_len = sizeof(char) + sizeof(_auth_method);
  bl2.append((char *)&_frame_len, sizeof(_frame_len));
  bl2.append(CEPH_MSGR_TAG_AUTH_SET_METHOD);
  bl2.append((char*)&_auth_method, sizeof(_auth_method));
  while ((ret = try_send(sd, bl2)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_CONNECTING_SEND_CONNECT_MSG
  bufferlist bl3;
  bl3.append((char*)&_stream_id, sizeof(_stream_id));
  _frame_len = sizeof(char) + sizeof(connect_msg);
  bl3.append((char*)&_frame_len, sizeof(_frame_len));
  bl3.append(CEPH_MSGR_TAG_CONN_MSG);

  bl3.append((char*)&connect_msg, sizeof(connect_msg));

  //omitted autherizor
  
  while((ret = try_send(sd, bl3)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_CONNECTING_WAIT_CONNECT_REPLY
  while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(_frame_len, sizeof(char) + sizeof(connect_reply));
  ASSERT_EQ(tag, CEPH_MSGR_TAG_CONN_REPLY);

  while ((ret = read_until(sd, sizeof(connect_reply), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);
  connect_reply = *((ceph_msg_connect_reply*)state_buffer);

  // STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH
  bufferlist authorizer_reply;
  if (connect_reply.authorizer_len) {
    ASSERT_LT(connect_reply.authorizer_len, 4096);
    while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(_frame_len, sizeof(char) + connect_reply.authorizer_len);
    ASSERT_EQ(tag, CEPH_MSGR_TAG_AUTH);

    // read connect reply auth
    while((ret = read_until(sd, connect_reply.authorizer_len, state_buffer)) > 0);
    ASSERT_EQ(ret, 0);
    authorizer_reply.append(state_buffer, connect_reply.authorizer_len);
    ASSERT_EQ(connect_reply.tag, CEPH_MSGR_TAG_SEQ);
  }

  // STATE_CONNECTING_WAIT_ACK_SEQ
  uint64_t newly_acked_seq = 0;
  while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(_frame_len, sizeof(char) + sizeof(newly_acked_seq));
  ASSERT_EQ(tag, CEPH_MSGR_TAG_SEQ);

  while((ret = read_until(sd, sizeof(newly_acked_seq), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);
  newly_acked_seq = *((uint64_t*)state_buffer);
  bufferlist bl4;
  uint64_t s = in_seq.read();
  bl4.append((char*)&_stream_id, sizeof(_stream_id));
  _frame_len = sizeof(char) + sizeof(s);
  bl4.append((char*)&_frame_len, sizeof(_frame_len));
  bl4.append(CEPH_MSGR_TAG_SEQ);
  bl4.append((char*)&s, sizeof(s));
  while((ret = try_send(sd, bl4)) > 0);
  ASSERT_EQ(ret, 0);

  server_msgr->shutdown();
  server_msgr->wait();
}

TEST_P(Msgrv2Test, FakeServerTest) {
  FakeDispatcher cli_dispatcher(false);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();
  int ret = -1;
  
  // below is fake server
  int listen_sd;
  __u32 _stream_id = 1024;  // sever set the stream_id
  __u32 _frame_len = -1;
  __u32 _auth_num = -1;
  char tag = 0;
  int sd = -1;
  entity_name_t name(entity_name_t::TYPE_OSD, 0);
  entity_addr_t bind_addr;
  entity_addr_t listen_addr;
  entity_addr_t socket_addr;

  bind_addr.parse("127.0.0.1:16800");

  ret = _bind(bind_addr, listen_addr, listen_sd);
  ASSERT_EQ(ret, 0);

  entity_inst_t inst(name, listen_addr);
  // This will start the async connection
  ConnectionRef conn = client_msgr->get_connection(inst);

  sd = _accept(listen_sd);
  ASSERT_GT(sd, 0);

  // STATE_ACCEPTING
  ret = set_nonblock(sd);
  ASSERT_EQ(ret, 0);

  set_socket_options(sd);

  char msgr2_banner[CEPH_MSGR2_BANNER_LEN+1];
  __u64 protocol_features_supported = 0;
  __u64 protocol_features_required = 0;
  sprintf(msgr2_banner, "ceph %16llx %16llx", protocol_features_supported, protocol_features_required);
  bufferlist bl;
  bl.append(msgr2_banner, strlen(msgr2_banner));

  ::encode(listen_addr, bl, 0);
  sockaddr_storage ss;
  socklen_t len = sizeof(ss);
  ret = ::getpeername(sd, (sockaddr*)&ss, &len);
  ASSERT_EQ(ret, 0);

  socket_addr.set_sockaddr((sockaddr*)&ss);
  ::encode(socket_addr, bl, 0);

  while ((ret = try_send(sd, bl)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_ACCEPTING_WAIT_BANNER_ADDR
  bufferlist addr_bl;
  entity_addr_t peer_addr;

  while ((ret = read_until(sd, CEPH_MSGR2_BANNER_LEN+sizeof(ceph_entity_addr), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);

  char recv_banner[CEPH_MSGR2_BANNER_LEN+1];
  strncpy(recv_banner, state_buffer, CEPH_MSGR2_BANNER_LEN);

  addr_bl.append(state_buffer+CEPH_MSGR2_BANNER_LEN, sizeof(ceph_entity_addr));
  {
    bufferlist::iterator ti = addr_bl.begin();
    ::decode(peer_addr, ti);
  }

  // send TAG_AUTH_METHODS
  bufferlist methods_bl;
  
  methods_bl.append((char*)&_stream_id, sizeof(_stream_id));
  _frame_len = sizeof(char) + (CEPH_AUTH_METHOD_NUM + 1)*sizeof(__u32);
  methods_bl.append((char *)&_frame_len, sizeof(_frame_len));
  methods_bl.append(CEPH_MSGR_TAG_AUTH_METHODS);
  _auth_num = CEPH_AUTH_METHOD_NUM;
  methods_bl.append((char*)&_auth_num, sizeof(__u32));
  for (__u32 i = 0; i < CEPH_AUTH_METHOD_NUM; i++)
    methods_bl.append((char*)&i, sizeof(__u32));

  while ((ret = try_send(sd, methods_bl)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_ACCEPTING_WAIT_AUTH_SET_METHOD
  while ((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(tag, CEPH_MSGR_TAG_AUTH_SET_METHOD);

  while((ret = read_until(sd, sizeof(__u32), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);

  // STATE_ACCEPTING_WAIT_CONNECT_MSG
  while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(_frame_len, sizeof(char) + sizeof(connect_msg));
  ASSERT_EQ(tag, CEPH_MSGR_TAG_CONN_MSG);

  while((ret = read_until(sd, sizeof(connect_msg), state_buffer)) > 0);
  ASSERT_EQ(ret, 0);

  connect_msg = *((ceph_msg_connect*)state_buffer);

  // STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH
  bufferlist authorizer_bl, authorizer_reply, reply_bl;
  if (connect_msg.authorizer_len) {
    while((ret = read_stream_id_and_frame_len_and_tag(sd, _stream_id, _frame_len, tag)) > 0);
    ASSERT_EQ(ret, 0);

    ASSERT_EQ(_frame_len, sizeof(char) + connect_msg.authorizer_len);
    ASSERT_EQ(tag, CEPH_MSGR_TAG_AUTH);

    while((ret = read_until(sd, connect_msg.authorizer_len, state_buffer)) > 0);
    ASSERT_EQ(ret, 0);
    authorizer_bl.append(state_buffer, connect_msg.authorizer_len);
  }

  
  memset(&connect_reply, 0, sizeof(connect_reply));
  connect_reply.features = 0;
  connect_reply.global_seq = connect_msg.global_seq + 1;
  connect_reply.connect_seq = connect_msg.connect_seq + 1;
  connect_reply.flags = 0;
  connect_reply.authorizer_len = 0;

  reply_bl.append((char*)&_stream_id, sizeof(_stream_id));
  _frame_len = sizeof(char) + sizeof(connect_reply);
  reply_bl.append((char*)&_frame_len, sizeof(_frame_len));
  reply_bl.append(CEPH_MSGR_TAG_CONN_REPLY);
  reply_bl.append((char*)&connect_reply, sizeof(connect_reply));

  // CEPH_MSGR_TAG_READY
  while((ret = try_send(sd, reply_bl)) > 0);
  ASSERT_EQ(ret, 0);
  
  client_msgr->shutdown();
  client_msgr->wait();
}

INSTANTIATE_TEST_CASE_P(
  Messenger,
  Msgrv2Test,
  ::testing::Values(
    "async"
  )
);

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  g_ceph_context->_conf->set_val("auth_cluster_required", "none");
  g_ceph_context->_conf->set_val("auth_service_required", "none");
  g_ceph_context->_conf->set_val("auth_client_required", "none");
  g_ceph_context->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features", "ms-type-async");
  g_ceph_context->_conf->set_val("ms_die_on_bad_msg", "true");
  g_ceph_context->_conf->set_val("ms_die_on_old_message", "true");
  g_ceph_context->_conf->set_val("ms_max_backoff", "1");
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 unittest_msgr && valgrind --tool=memcheck ./unittest_msgr"
 * End:
 */
