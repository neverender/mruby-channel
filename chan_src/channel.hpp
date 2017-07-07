struct Channel {

  std::string name;
  std::mutex mtx;
  std::condition_variable cv;
  std::atomic<std::uint32_t> refcount;
  std::queue<Variant> queue;
  std::atomic<bool> is_close;

  static std::mutex named_channels_mtx;
  static std::map<std::string, Channel*> named_channels;
  static Channel* get_channel(const std::string& name);

  Channel();
  ~Channel();

  void add_ref();
  void release();

  bool push(Variant& val);

  bool try_pop(Variant* ret);
  bool pop(Variant* ret);

  void close();

};
