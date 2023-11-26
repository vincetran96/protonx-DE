# Bài tập 2
# Hướng dẫn làm bài

Các bạn sẽ thực hiện `code` ở giữa các phần hiển thị `#TODO: Begin` và `#TODO: End.`

## Yêu cầu cài đặt

- Nếu là máy Windows thì cần cài đặt WSL2:
    - https://learn.microsoft.com/en-us/windows/wsl/install
- gcloud:
    - https://cloud.google.com/sdk/docs/install
- docker:
    - https://www.docker.com/

Chạy lệnh sau để authenticate vào tài khoảng gcloud 

```bash
gcloud auth login
```

và 

```bash
gcloud auth application-default login
```

## Tạo môi trường ảo và setup thư viện
Tại folder assignment_2

Chạy lệnh dể build docker và tạo môi trường .venv ảo
```bash
make setup  
```

## Cách chạy test

```bash
make test
```

## Tắt Docker Compose 
```bash
make docker_down
```

## Bật Docker Compose
```bash
make docker_up
```

## Clean môi trường 
```bash
make clean
```