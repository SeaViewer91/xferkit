# xferkit

[English](https://github.com/SeaViewer91/xferkit/blob/main/README.md) | **한국어**

**파이썬 서버용 분할·이어받기 대용량 파일 업로드/다운로드 SDK — FastAPI 바로 연동, 브라우저 클라이언트 포함.**

xferkit은 수십~수백 GB 단위의 대용량 파일을 일반 HTTP로 주고받습니다.

- **병렬·이어받기 업로드.** 파일을 여러 파트로 나눠 동시에 보냅니다. 업로드가 끊기면 클라이언트 프로세스를 다시 시작하거나 페이지를 새로고침해도 멈춘 지점부터 이어서 진행합니다.
- **병렬·이어받기 다운로드.** HTTP Range 요청과 `ETag`/`If-Range` 검증을 사용하고, 원격 파일이 바뀌면 처음부터 자동으로 다시 받습니다.
- **처음부터 끝까지 무결성 검증.** 파트가 도착할 때마다 SHA-256으로 검증하고, 전송이 끝나면 양쪽이 파일 전체를 다시 읽지 않고도 합성 체크섬을 비교합니다.
- **폴더와 대량의 작은 파일.** 폴더 전체(또는 수천 개의 파일)를 파일별 결과와 함께 올립니다. 작은 파일은 파일당 요청 한 번으로 끝나고, 다시 실행하면 이미 올라간 파일은 건너뜁니다.
- **정확히 한 번.** 세션 생성이 멱등적이고, `idempotency_key`로 앱의 키 하나를 업로드 하나에 대응시킬 수 있습니다. 중간에 죽거나 재시도해도 중복이 생기지 않습니다.
- **접근 제어 내장.** 업로드는 그것을 만든 호출자의 소유가 됩니다. 서명된 grant(presigned URL)를 쓰면 브라우저나 작업 서버에 범위와 만료 시간이 정해진 권한을 줄 수 있습니다.
- **부하에 강한 운영.** 디스크 공간 확보, 사용자별 제한, 요청 속도 제한, 완료 기록 보관 정책을 갖췄고, 장애 주입 테스트로 검증된 강제 종료 안전 상태 관리를 합니다.
- **로컬 디스크 또는 S3.** 파트를 디스크의 최종 위치에 바로 기록하거나(100GB 업로드 완료가 복사가 아니라 이름 변경 한 번), S3 호환 버킷에 멀티파트 업로드로 기록합니다.
- **프레임워크에 독립적인 코어.** 서버 코어는 외부 의존성이 없습니다. FastAPI는 연동 방식 중 하나일 뿐이고, Flask·Django·aiohttp나 직접 만든 서버에서도 같은 `UploadManager`를 호출하면 됩니다.
- **멀티 워커 환경에서 안전.** 데이터베이스 없이도 uvicorn/gunicorn 워커 여러 개가 같은 저장 폴더를 공유할 수 있습니다.

xferkit은 GitHub에서 바로 설치합니다(아직 PyPI에는 없습니다). 필요한 extra를 고르고 릴리스 태그를 지정하세요.

```
pip install "xferkit[fastapi] @ git+https://github.com/SeaViewer91/xferkit.git@v0.1.0"   # FastAPI 서버
pip install "xferkit[client] @ git+https://github.com/SeaViewer91/xferkit.git@v0.1.0"    # 파이썬 클라이언트 + CLI
pip install "xferkit[s3] @ git+https://github.com/SeaViewer91/xferkit.git@v0.1.0"        # S3 호환 저장소(boto3)
pip install "xferkit[all] @ git+https://github.com/SeaViewer91/xferkit.git@v0.1.0"       # 위의 전부
```

`requirements.txt`에는 `xferkit[fastapi] @ git+https://github.com/SeaViewer91/xferkit.git@v0.1.0` 형식으로 적습니다. 각 [릴리스](https://github.com/SeaViewer91/xferkit/releases)에는 wheel 파일도 첨부됩니다.

Python 3.10 이상이 필요합니다(Node.js는 필요 없음). 브라우저 클라이언트는 파이썬 패키지에 함께 들어 있습니다. [브라우저 클라이언트](#브라우저-클라이언트) 절을 참고하세요.

## 서버 (FastAPI)

```python
import os
from fastapi import FastAPI, Request
from xferkit import GrantSigner, LocalStorage, UploadManager
from xferkit.integrations.fastapi import add_cors, create_upload_router, mount_browser_client

manager = UploadManager(
    LocalStorage("/data/xferkit", min_free_bytes=10 * 1024**3),  # 마지막 10GB는 남겨 두도록 업로드 거절
    max_upload_size=500 * 1024**3,
    ttl=24 * 3600,                  # 완료되지 않은 업로드는 하루 뒤 만료
    max_pending_per_owner=20,       # 사용자별 동시 진행 업로드 수
    completed_ttl=7 * 24 * 3600,    # 완료 기록 보관 기간
)
signer = GrantSigner(os.environ["XFERKIT_GRANT_SECRET"])

async def current_user(request: Request) -> str | None:
    ...  # 기존 인증 방식(세션 쿠키, JWT, API 키 등)으로 사용자 id를 돌려줌(없으면 None)

async def on_complete(completed, request: Request):
    # completed.path는 조립이 끝난 파일, completed.owner는 업로드한 사용자
    ...

app = FastAPI()
app.include_router(
    create_upload_router(
        manager,
        prefix="/uploads",
        principal=current_user,   # 누가 호출했는지
        grants=signer,            # 서명된 grant도 받음
        require_auth=True,        # 익명 업로드 금지
        on_complete=on_complete,
        expose_downloads=True,    # GET /uploads/{id}/content (Range 지원)
    )
)
mount_browser_client(app)                       # /xferkit.mjs 제공
add_cors(app, ["https://app.example.com"])      # 웹앱이 다른 출처(origin)에 있을 때
```

방치된 세션, 만료된 기록, 강제 종료로 남은 흔적을 지우려면 `manager.cleanup_expired()`를 주기적으로(예: 1시간마다) 호출하세요. 드래그 앤 드롭 브라우저 데모가 포함된 실행 예제는 [examples/server_app.py](https://github.com/SeaViewer91/xferkit/blob/main/examples/server_app.py)에 있습니다.

### 접근 제어

- **소유권.** 업로드는 그것을 만든 호출자(`principal`이 돌려준 값 또는 grant의 owner)의 소유입니다. 다른 호출자에게는 `404`가 돌아가므로 업로드 id를 추측해 찔러볼 수 없습니다. owner 없이 만든 업로드는 id를 아는 사람이면 누구나 접근할 수 있습니다.
- **서명된 grant.** 앱이 짧은 수명과 범위를 가진 토큰을 발급하고, 전송 서버는 공유 비밀키로 검증만 합니다. 서버에 저장되는 상태는 없습니다.

  ```python
  # alice가 1시간 동안 /uploads로 20GB 이하 파일을 올릴 수 있게:
  grant = signer.issue(owner="alice", prefix="/uploads", methods="*", ttl=3600, max_size=20 * 1024**3)

  # 파일 하나에 대한 10분짜리 다운로드 링크(<a href>, curl 등 어디서나 동작):
  url = signer.sign_url(f"https://files.example.com/uploads/{upload_id}/content", ttl=600)
  ```

  grant는 `X-Xferkit-Grant` 헤더나 `xk` 쿼리 파라미터로 전달합니다. 비밀키를 교체하는 동안에는 이전 키도 유효하게 둘 수 있습니다(`previous_secrets=`).
- **정책.** `authorize=`로 작업별(`create`, `upload_part`, `complete`, `abort`, `download`, `status`) 확인을 직접 넣을 수 있습니다. 매니저의 `before_create=`로 업로드를 거절할 수 있고(용량 할당, 파일 형식 등), `rate_limit=RateLimiter(rate, burst)`를 걸면 `429`와 `Retry-After`로 응답합니다. 모든 클라이언트는 이 `Retry-After`를 따릅니다.

### S3 호환 저장소

```python
from xferkit.server.s3 import S3Storage

manager = UploadManager(S3Storage("my-bucket", endpoint_url="https://s3.example.com"))  # AWS, MinIO, Ceph, R2 등
```

업로드 하나가 S3 멀티파트 업로드 하나가 되어 버킷 안에서 조립됩니다. 파트는 S3로 보내기 전에 검증하고, 모든 상태를 버킷에 두므로 서버 여러 대가 한 버킷을 같이 쓸 수 있습니다. `completed.location`은 `s3://bucket/key`이고, 다운로드는 presigned URL로 리다이렉트됩니다. 파트는 최소 5MiB입니다(S3 규칙). 브라우저가 버킷에서 직접 내려받는다면 버킷에 CORS 규칙을 추가하세요.

### 디스크 공간과 보관 정책

- 새 업로드는 그 크기와 진행 중인 업로드들이 앞으로 더 필요로 할 공간을 합쳐도 `min_free_bytes` 위에 들어맞아야 받습니다. 아니면 `507 insufficient_storage`로 거절합니다. `preallocate=True`(리눅스)를 켜면 공간을 처음부터 실제로 할당합니다.
- 완료 기록(강제 종료된 클라이언트가 이미 끝난 업로드를 알아보는 데 필요)은 `completed_ttl`이 지나면 지웁니다. 파일까지 지우려면 `remove_files_on_expiry=True`로 설정하세요.

## 파이썬 클라이언트

```python
from xferkit.client import upload_file, download_file

result = upload_file(
    "https://files.example.com/uploads",
    "survey_2026.tif",
    metadata={"project": "coastal"},
    grant=grant,                       # 또는 headers={"Authorization": "Bearer ..."}
    concurrency=6,
    state_dir="~/.cache/xferkit",      # 실행이 끊겨도 이어서 올릴 수 있게 함
    idempotency_key="coastal/survey_2026.tif",  # 다시 실행해도 같은 파일을 두 번 올리지 않음
)
print(result.upload_id, result.composite_checksum)

download_file(url, "copy.tif", concurrency=6)
```

폴더와 여러 파일:

```python
from xferkit.client import upload_folder

result = upload_folder(
    "https://files.example.com/uploads", "survey_2026/",
    include="*.JPG", metadata={"project": "coastal"}, grant=grant, concurrency=8,
)
for failure in result.failed:
    print(failure.path, failure.error)
```

각 파일은 원래 이름을 유지하고 `metadata["relative_path"]`에 폴더 안 경로가 담깁니다. 같은 호출을 다시 실행하면 끝나지 않은 파일은 이어서 올리고, 끝난 파일은 검증만 하고 건너뜁니다. 경로 목록에는 `upload_many([...])`를 씁니다.

비동기 코드에서는 `AsyncUploader` / `AsyncDownloader`를 직접 사용합니다. 다운로더는 Range 요청을 지원하는 모든 서버(nginx, S3, CDN 등)에서 동작합니다.

## 브라우저 클라이언트

```html
<script type="module">
  import { upload, download } from "/xferkit.mjs";   // mount_browser_client(app)가 제공

  const { grant } = await (await fetch("/api/upload-grant")).json();   // 앱이 발급
  const result = await upload(fileInput.files[0], {
    baseUrl: "https://files.example.com/uploads",
    grant,
    onProgress: (p) => (bar.value = p.fraction),
  });
</script>
```

폴더 전체(`<input type="file" webkitdirectory>`)나 폴더가 포함된 드래그 앤 드롭:

```js
import { uploadFiles, filesFromDataTransfer } from "/xferkit.mjs";

const batch = await uploadFiles(folderInput.files, { baseUrl, grant, onProgress });
dropZone.ondrop = async (e) => { e.preventDefault(); await uploadFiles(await filesFromDataTransfer(e.dataTransfer), { baseUrl, grant }); };
```

파이썬 클라이언트와 같은 것을 보장합니다. 병렬 파트 전송, 파트별 SHA-256, 합성 체크섬 검증, 새로고침이나 `AbortController.abort()` 이후 이어받기, 멱등성 키(파이썬과 공유)를 지원합니다. 자세한 내용은 [clients/js](https://github.com/SeaViewer91/xferkit/blob/main/clients/js/README.md)를 참고하세요.

## CLI

```
xferkit upload big.tif https://example.com/uploads -c 6 --grant "$GRANT"
xferkit upload ./survey_2026 https://example.com/uploads --include '*.JPG'   # 폴더
xferkit download "https://example.com/uploads/abc/content?xk=..." ./big.tif -c 6
```

Ctrl+C로 중단한 뒤 같은 명령을 다시 실행하면 이어서 진행합니다.

## 다른 프레임워크에서 사용하기

`UploadManager`는 일반적인 동기 파이썬 코드입니다. 프로토콜의 다섯 가지 엔드포인트를 사용하는 프레임워크에 연결하고, `manager.ensure_access(upload_id, caller)`로 접근 권한을 확인한 뒤, 파트 본문을 `manager.open_part()`로 흘려 넣으면 됩니다.

```python
with manager.open_part(upload_id, index) as sink:
    for block in request_body_iterator:
        sink.write(block)
    record = sink.commit(request.headers.get("X-Xferkit-Checksum"))
```

Range 다운로드는 `xferkit.server.plan_file_response()`와 `iter_file_range()`로, grant 검증은 `GrantSigner.verify()`로 같은 방식으로 처리합니다.

## 동작 방식

전송 프로토콜은 [docs/PROTOCOL.md](https://github.com/SeaViewer91/xferkit/blob/main/docs/PROTOCOL.md)에, 저장 구조·보안 모델과 각각의 보장 사항은 [docs/DESIGN.md](https://github.com/SeaViewer91/xferkit/blob/main/docs/DESIGN.md)에 정리되어 있습니다.

## 개발 환경

```
./scripts/setup_venv.sh          # macOS / Linux  (Windows: scripts\setup_venv.ps1)
source .venv/bin/activate
pytest                           # Playwright와 Chromium이 설치돼 있으면 브라우저 테스트도 실행
ruff check src tests && ruff format --check src tests
cd clients/js && npm install && npm run build    # 브라우저 클라이언트를 수정한 뒤
```

부하 테스트(약 10MB 이미지 수천 장, 동시 세션 10개, 장애 주입)는 [benchmarks/load](https://github.com/SeaViewer91/xferkit/blob/main/benchmarks/load/README.md)를 참고하세요.

## 라이선스

MIT

---

> 이 문서는 [README.md](https://github.com/SeaViewer91/xferkit/blob/main/README.md)(영문)의 번역본입니다. 내용을 바꿀 때는 두 파일을 함께 수정해 주세요.
