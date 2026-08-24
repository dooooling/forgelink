# S7comm 协议参考（基于 Wireshark 官方 dissector 源码核对）

> 出处：[packet-s7comm.c](https://github.com/wireshark/wireshark/blob/master/epan/dissectors/packet-s7comm.c)
> （Thomas Wiens 维护的官方 Wireshark dissector，master 分支）。
> 本文档供 `drivers/s7comm` 真机调试与后续维护参考；常量与帧布局以此为准。

## 1. 帧层次

```text
TPKT (RFC 1006) → COTP (ISO 8073) → S7 PDU
```

## 2. S7 PDU 头（所有 ROSCTR 共用前 10 字节）

```text
[0x32][ROSCTR u8][red-id u16][pdu-ref u16][参数长 u16][数据长 u16]
```

- ROSCTR：Job=0x01 / Ack=0x02 / **Ack_Data=0x03** / Userdata=0x07
- red-id 恒为 0x0000

Ack_Data 头在上述 10 字节后追加 **error-class u8 + error-code u8**（共 12 字节）。

## 3. Setup Communication（function 0xF0）

**应答参数区布局（dissector `s7comm_decode_pdu_setup_communication`，权威）**：

```text
偏移 0: reserved   u8
偏移 1: max-amq-calling u16 BE
偏移 3: max-amq-called  u16 BE
偏移 5: 协商 PDU 长度    u16 BE
```

共 8 字节——**PDU 长度在 [5..7]**，不在 [2..4]。

## 4. Read Var 应答（Ack_Data, function 0x04）

### 参数区

```text
偏移 0: function = 0x04
偏移 1: item count u8
```

**共 2 字节，无 reserved 字节**——item count 直接跟在功能码后
（dissector 中 ACK_DATA 分支：读 function 后 `offset += 1` 即取 item count）。

### 数据区（每 item，dissector `s7comm_decode_response_read_data`）

```text
偏移 0: return code u8（0xFF 成功）
偏移 1: transport size u8
偏移 2: length u16 BE —— BIT 类按位计数（向上取整到字节），其余按字节
偏移 4: 载荷
尾部: 非 2 的倍数时补 1 字节 pad（最后一个 item 不补）
```

**transport size 表**：

| 值 | 名称 | length 单位 |
|---|---|---|
| 1 | BIT | 位 |
| 3 | BYTE/CHAR | 字节 |
| 4 | WORD | 字节 |
| 6 | DWORD | 字节 |
| 8 | REAL | 字节 |

注意 dissector 源码中 BIT 按"位"换算（`len % 8` 向上取整除 8），其余按字节直取。

## 5. 对本驱动的修正结论

1. **Setup 应答解析**：真实 PLC 参数区 8 字节、PDU 在 [5..7]。原实现取 [2..4]
   （即 max-amq-calling 字段，真机典型值 1）导致"PDU 过小"误判。已修复为
   ≥8 字节按完整布局取 [5..7]、精简固件 ≥4 字节回退短布局取 [2..4]。
2. **Read 应答头校验**：`check_read_header` 要求参数区 ≥4 字节是错误的——
   真实应答参数区只有 2 字节（function + item count）。条件应为 `< 2`，
   且报错信息中"期望 0x04 收到 0x04"的自相矛盾正是 `param.len() < 4`
   触发后打印了首字节所致。已修复为 `< 2`。
