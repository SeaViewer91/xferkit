# xferkit

[English](README.md) | **한국어**

**파이썬 서버용 분할·이어받기 대용량 파일 업로드/다운로드 SDK — FastAPI 바로 연동.**

xferkit은 수십~수백 GB 단위의 대용량 파일을 일반 HTTP로 주고받습니다.

- **병렬·이어받기 업로드.** 파일을 여러 파트로 나눠 동시에 보냅니다. 업로드가 중간에 끊기면 클라이언트 프로세스를 다시 시작하더라도 멈춘 지점부터 이어서 진행합니다.
- **병렬·이어받기 다운로드.** HTTP Range 요청과 `ETag`/`If-Range` 검증을 사용하고, 원격 파일이 바뀌면 처음부터 자동으로 다시 받습니다.
- **처음부터 끝까지 무결성 검증.** 파트가 도착할 때마다 SHA-256으로 검증하고, 전송이 끝나면 양쪽이 파일 전체를 다시 읽지 않고도 합성 체크섬을 비교합니다.
- **디스크를 두 배로 쓰지 않음.** 파트를 최종 위치에 바로 기록하므로 100GB 업로드를 완료하는 작업은 100GB 복사가 아니라 파일 이름 변경 한 번입니다.
- **프레임워크에 독립적인 코어.** 서버 코어는 외부 의존성이 없습니다. FastAPI는 연동 방식 중 하나일 뿐이고, Flask·Django·aiohttp나 직접 만든 서버에서도 같은 `UploadManager`를 호출하면 됩니다.
- **멀티 워커 환경에서 안전.** 상태를 파트별 마커 파일과 원자적 이름 변경으로 관리하므로, 데이터베이스 없이도 uvicorn/gunicorn 워커 여러 개가 같은 저장 폴더를 공유할 수 있습니다.

```
pip install "xferkit[fastapi]"   # FastAPI 서버
pip install "xferkit[client]"    # 클라이언트 + CLI
pip install "xferkit[all]"       # 둘 다
```

Python 3.10 이상이 필요합니다.

## 서버 (FastAPI)

```python
from fastapi import Depends, FastAPI, Request
from xferkit import UploadManager
from xferkit.integrations.fastapi import create_upload_router, range_file_response

manager = UploadManager(
    "/data/xferkit",            # 저장 루트 (하나의 파일시스템 안에 있어야 함)
    max_upload_size=500 * 1024**3,
    ttl=24 * 3600,              # 완료되지 않은 세션은 하루 뒤 만료
)

async def on_complete(completed, request: Request):
    # completed.path가 조립이 끝난 파일입니다. 옮기거나 DB에 등록하는 등의 처리를 여기서 합니다.
    print(completed.upload_id, completed.filename, completed.size, completed.path)

app = FastAPI()
app.include_router(
    create_upload_router(
        manager,
        prefix="/uploads",
        dependencies=[Depends(require_user)],   # 모든 엔드포인트에 인증 적용
        on_complete=on_complete,
    )
)

# Range / If-Range / ETag를 지원하는 파일 제공 (이어받기·병렬 다운로드 가능)
@app.head("/files/{name}", include_in_schema=False)
@app.get("/files/{name}")
async def get_file(name: str, request: Request):
    return range_file_response(request, resolve_safely(name))
```

방치된 세션을 지우려면 스케줄러 등으로 `manager.cleanup_expired()`를 주기적으로 호출하세요. 바로 실행해 볼 수 있는 서버는 [examples/server_app.py](examples/server_app.py)에 있습니다.

## 클라이언트

```python
from xferkit.client import upload_file, download_file

result = upload_file(
    "https://example.com/uploads",
    "survey_2026.tif",
    metadata={"project": "coastal"},
    headers={"Authorization": "Bearer ..."},
    concurrency=6,
    state_dir="~/.cache/xferkit",      # 실행이 끊겨도 이어서 올릴 수 있게 함
)
print(result.upload_id, result.composite_checksum)

download_file("https://example.com/files/survey_2026.tif", "copy.tif", concurrency=6)
```

비동기 코드에서는 같은 객체를 직접 사용합니다.

```python
from xferkit.client import AsyncUploader, AsyncDownloader

async with AsyncUploader("https://example.com/uploads", concurrency=8) as uploader:
    result = await uploader.upload("big.bin", progress=lambda p: print(f"{p.fraction:.1%}"))

async with AsyncDownloader(concurrency=8, segment_size=32 * 1024**2) as downloader:
    await downloader.download("https://example.com/files/big.bin", "big.bin")
```

다운로더는 xferkit 서버뿐 아니라 Range 요청을 지원하는 모든 서버(nginx, S3, CDN 등)에서 동작합니다.

## CLI

```
xferkit upload big.tif https://example.com/uploads -c 6 -H "Authorization: Bearer ..."
xferkit download https://example.com/files/big.tif ./big.tif -c 6
```

Ctrl+C로 중단한 뒤 같은 명령을 다시 실행하면 이어서 진행합니다.

## 다른 프레임워크에서 사용하기

`UploadManager`는 일반적인 동기 파이썬 코드입니다. 프로토콜의 다섯 가지 엔드포인트를 사용하는 프레임워크에 연결하고, 파트 본문을 `manager.open_part()`로 흘려 넣으면 됩니다.

```python
with manager.open_part(upload_id, index) as sink:
    for block in request_body_iterator:
        sink.write(block)
    record = sink.commit(request.headers.get("X-Xferkit-Checksum"))
```

Range 다운로드는 `xferkit.server.plan_file_response()`와 `iter_file_range()`로 같은 방식으로 처리합니다.

## 동작 방식

전송 프로토콜은 [docs/PROTOCOL.md](docs/PROTOCOL.md), 저장 구조와 보장 사항은 [docs/DESIGN.md](docs/DESIGN.md)에 정리되어 있습니다.

## 개발 환경

```
./scripts/setup_venv.sh          # macOS / Linux  (Windows: scripts\setup_venv.ps1)
source .venv/bin/activate
pytest
ruff check src tests && ruff format --check src tests
```

## 라이선스

MIT

---

> 이 문서는 [README.md](README.md)(영문)의 번역본입니다. 내용을 바꿀 때는 두 파일을 함께 수정해 주세요.
