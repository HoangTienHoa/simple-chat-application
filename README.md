# Kafka Chat Demo

Web chat đơn giản cho 2 user, dùng Kafka làm message broker. Demo cách **chọn partition dựa trên hash của userID**: mỗi user luôn gửi tin vào cùng một partition của topic `chat`.

## Chức năng

- Kafka chạy một node ở chế độ KRaft (không cần ZooKeeper), có topic `chat` với **2 partition**.
- Web chat cho 2 user cố định là `user1` và `user2`. Tin nhắn hiển thị realtime qua WebSocket (Socket.IO).
- **Producer:** khi user gửi tin, server tính partition rồi gửi tin vào đúng partition đó:

  ```
  partition = |hash(userId)| % 2
  ```

  Hàm `hash` tính giống `String.hashCode()` của Java:

  | User  | hash      | Partition |
  |-------|-----------|-----------|
  | user1 | 111578566 | 0         |
  | user2 | 111578567 | 1         |

- **Consumer:** đọc cả 2 partition rồi đẩy tin đến mọi trình duyệt đang mở. Mỗi tin nhắn hiện partition và offset của nó (P0 màu xanh dương, P1 màu xanh lá).
- **Lịch sử chat:** mỗi lần server khởi động, consumer dùng group id mới và đọc topic từ đầu, nên lịch sử chat được nạp lại.
- **Kafka UI:** xem topic, partition, tin nhắn và consumer group qua giao diện web.

## Kiến trúc

```
Trình duyệt user1 ─┐                              ┌─► partition 0 (user1) ─┐
                   ├─► server.js ── producer ──►──┤                        │
Trình duyệt user2 ─┘   (Socket.IO)    topic chat  └─► partition 1 (user2) ─┤
        ▲                                                                  │
        └────────── Socket.IO ◄── server.js consumer ◄─────────────────────┘
```

| Thành phần | Địa chỉ | Ghi chú |
|---|---|---|
| Web chat (`server.js`) | http://localhost:3000 | Node.js, chạy trên máy host |
| Kafka broker | `localhost:9094` | Cho app trên máy host |
| Kafka broker (nội bộ) | `kafka:29092` | Cho các container khác, ví dụ Kafka UI |
| Kafka UI | http://localhost:8081 | Image `kafbat/kafka-ui` |

## Cấu trúc thư mục

```
.
├── docker-compose.yml   # Kafka, kafka-init (tạo topic), kafka-ui
├── server.js            # Express + Socket.IO + KafkaJS (producer & consumer)
├── public/index.html    # Giao diện chat
├── Dockerfile           # Image của web chat
├── .github/workflows/docker-publish.yml  # CI/CD: build, push image lên GHCR, deploy
├── deploy/
│   ├── docker-compose.prod.yml  # Stack chạy trên server
│   └── remote-deploy.sh         # Script workflow chạy trên server qua SSH
└── package.json
```

## Yêu cầu

- Docker và Docker Compose
- Node.js 18 trở lên

## Các bước chạy

1. **Khởi động Kafka, tạo topic và chạy Kafka UI**

   ```bash
   docker compose up -d
   ```

   Service `kafka-init` chờ Kafka sẵn sàng rồi tạo topic `chat` với 2 partition. Kiểm tra bằng lệnh:

   ```bash
   docker compose logs kafka-init
   # Topic: chat  PartitionCount: 2  ReplicationFactor: 1 ...
   ```

2. **Cài dependencies**

   ```bash
   npm install
   ```

3. **Chạy web chat**

   ```bash
   npm start
   ```

   Khi chạy thành công, log sẽ hiện:

   ```
   Topic "chat" có 2 partition
   Web chat: http://localhost:3000
     user1: hash=111578566 -> partition 0
     user2: hash=111578567 -> partition 1
   ```

4. **Chat:** mở 2 tab trình duyệt, mỗi tab là một user:

   - http://localhost:3000/?user=user1
   - http://localhost:3000/?user=user2

   Bạn cũng có thể đổi user bằng ô chọn trên thanh tiêu đề.

5. **Xem dữ liệu trong Kafka UI:** mở http://localhost:8081

   - **Topics → chat → Messages:** xem từng tin nhắn kèm partition, offset và key (userId).
   - **Consumers:** xem consumer group `web-chat-<timestamp>` của server.

## Cấu hình

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `3000` | Port của web chat |
| `KAFKA_BROKER` | `localhost:9094` | Địa chỉ Kafka broker |

Ví dụ: `PORT=4000 npm start`

## Docker image & CI

Workflow [.github/workflows/docker-publish.yml](.github/workflows/docker-publish.yml) build image của web chat (theo [Dockerfile](Dockerfile)) cho `linux/amd64` và `linux/arm64`, rồi push lên GitHub Container Registry:

| Sự kiện | Kết quả |
|---|---|
| Push lên `master` | Push image với tag `latest` và `sha-<commit>` |
| Push tag `v1.2.3` | Push image với tag `1.2.3` và `1.2` |
| Pull request vào `master` | Chỉ build để kiểm tra, không push |
| Chạy tay (Actions → Run workflow) | Build và push theo branch được chọn |

Workflow dùng `GITHUB_TOKEN` có sẵn nên không cần tạo secret. Image nằm ở `ghcr.io/<owner>/<repo>`, mặc định là **private**. Muốn đổi sang public, vào **Packages → Package settings** trên GitHub.

Chạy image cùng Kafka trong `docker-compose.yml`:

```bash
docker compose up -d
docker run --rm -p 3000:3000 \
  --network kafka-demo_default \
  -e KAFKA_BROKER=kafka:29092 \
  ghcr.io/<owner>/<repo>:latest
```

Container nằm cùng network với Kafka, nên phải dùng listener nội bộ `kafka:29092` thay cho `localhost:9094`.

## Tự động deploy lên server

Job `deploy` trong workflow chạy sau khi `build` xong, mỗi lần push lên `master`:

1. SSH vào server và copy [deploy/docker-compose.prod.yml](deploy/docker-compose.prod.yml) thành `~/kafka-chat/docker-compose.yml`.
2. Chạy [deploy/remote-deploy.sh](deploy/remote-deploy.sh) trên server:
   - ghi `APP_IMAGE=ghcr.io/<owner>/<repo>:sha-<commit>` vào `.env`;
   - chạy `docker compose pull app` rồi `docker compose up -d`. Chỉ container `app` được tạo lại, Kafka vẫn chạy nguyên;
   - chờ app trả lời `/api/users`. Quá 60 giây không có phản hồi thì in log và báo lỗi.

Job này **tự bỏ qua** cho đến khi bạn cấu hình xong các bước dưới đây.

### Bước 1: Chuẩn bị server
- Server Linux đã cài Docker và Docker Compose plugin (`docker compose version` chạy được).
- User dùng để deploy chạy được lệnh `docker` mà không cần sudo: `sudo usermod -aG docker <user>`, sau đó đăng nhập lại.
- Mở port `3000` trên firewall, ví dụ `sudo ufw allow 3000`.

### Bước 2: Tạo SSH key riêng cho deploy (chạy trên máy bạn)
```bash
ssh-keygen -t ed25519 -f ~/.ssh/kafka_chat_deploy -N "" -C "github-actions-deploy"
ssh-copy-id -i ~/.ssh/kafka_chat_deploy.pub <user>@<server>
ssh -i ~/.ssh/kafka_chat_deploy <user>@<server> 'docker ps'   # kiểm tra
```

### Bước 3: Khai báo trên GitHub
Vào repo → **Settings → Secrets and variables → Actions**:

| Loại | Tên | Giá trị |
|---|---|---|
| Secret | `DEPLOY_SSH_KEY` | Nội dung file `~/.ssh/kafka_chat_deploy` (private key, kể cả dòng `-----BEGIN/END-----`) |
| Variable | `DEPLOY_HOST` | IP hoặc domain của server |
| Variable | `DEPLOY_USER` | User SSH |
| Variable | `DEPLOY_PORT` | Port SSH. Không bắt buộc, mặc định `22` |

### Bước 4: Push lên `master`
Mở tab **Actions** để xem: job `build` chạy trước, sau đó đến `deploy`. Khi deploy xong:
- Web chat: `http://<server>:3000`
- Kafka UI không mở ra Internet, vì không có đăng nhập. Mở bằng SSH tunnel:
  ```bash
  ssh -L 8081:localhost:8081 <user>@<server>
  # rồi mở http://localhost:8081 trên máy bạn
  ```

### Trên server
- **Dữ liệu Kafka** nằm trong volume `kafka-chat_kafka-data`, còn nguyên khi deploy lại hay reboot. Chỉ mất khi chạy `docker compose down -v`.
- **Đổi port:** thêm `APP_PORT=80` hoặc `UI_PORT=...` vào `~/kafka-chat/.env`. Script deploy chỉ ghi đè dòng `APP_IMAGE`, các dòng khác giữ nguyên.
- **Rollback:**
  ```bash
  cd ~/kafka-chat && sed -i 's/^APP_IMAGE=.*/APP_IMAGE=ghcr.io\/<owner>\/<repo>:sha-<commit cũ>/' .env && docker compose up -d
  ```
  Cách khác: chạy lại workflow của commit cũ trong tab Actions.
- **Image private:** nếu package GHCR là private, chạy `docker login ghcr.io` một lần trên server bằng token có quyền `read:packages`.

## Dừng project

```bash
# Dừng web chat: nhấn Ctrl+C trong terminal đang chạy npm start
docker compose down
```

## Lưu ý

- **Thứ tự tin nhắn:** Kafka chỉ đảm bảo thứ tự **trong từng partition**. Tin của cùng một user luôn đúng thứ tự, nhưng tin giữa `user1` và `user2` có thể hiện xen kẽ không đúng thứ tự gửi.
- **Dữ liệu không được lưu lại:** Kafka chưa có volume, nên `docker compose down` hoặc tạo lại container Kafka sẽ xóa toàn bộ tin nhắn. Lần `up` tiếp theo, topic sẽ được tạo lại.
- **Port:** Kafka dùng `9094` và Kafka UI dùng `8081` để tránh trùng với các Kafka khác hay chạy ở `9092` và `8080`. Nếu cần đổi, sửa trong `docker-compose.yml`, và đổi cả `KAFKA_BROKER` nếu đổi port Kafka.
- **Cảnh báo `TimeoutNegativeWarning`:** đây là lỗi đã biết của KafkaJS khi chạy trên Node 24, không ảnh hưởng chức năng.
