//! local-buffer：离线缓存与 Store-and-Forward（占位）。
//!
//! 内存队列 + 磁盘 WAL / 嵌入式 DB 两级缓存（§102、§103），网络恢复后补传。
