# Device Profiles

此目录存放设备型号的 Profile 定义（JSON，架构方案 §37、§38），按品牌或型号族分目录。
Profile 描述设备语义与协议地址的映射，不复制协议 Driver；使用已有协议时应新增
Profile，而不是新增 Driver。

## 目录约定

```text
profiles/
├── inovance/
│   └── md500.json
├── siemens/
└── README.md
```

当前仓库尚未提交可供运行时加载的正式 Profile 文件；`profile-engine` 已提供 JSON
加载、完整校验、注册和读写转换能力。新增 Profile 前应先运行：

```bash
cargo test -p profile-engine --all-features
```

具体字段、路径命名、缩放/单位、枚举和写入逆转换规则以
[架构设计方案](../Rust工业IoT采集平台架构设计方案.md) §37、§37.1 为准。
