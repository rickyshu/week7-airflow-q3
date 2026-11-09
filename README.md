# 7주차 Airflow 과제

- 이름: 이호인
- 과정: 데이터 엔지니어링 부트캠프 8기

이 저장소의 `docker-compose.yaml`은 Apache Airflow 공식 Docker Compose 빠른 시작 파일을 기반으로 합니다. `dags/sample_dag.py`는 Q3의 일일 실행 DAG입니다.

## 로컬 실행

```sh
mkdir -p dags logs plugins config
docker compose up airflow-init
docker compose up -d
```

Airflow 화면은 <http://localhost:8080>에서 확인합니다. 실습용 기본 계정은 `airflow` / `airflow`입니다. Docker Compose 빠른 시작 구성은 학습용이며 운영 환경 배포용이 아닙니다.

과제 증빙 캡처와 Git 변경 기록은 저장소 밖의 `../Q3/`에 보관합니다.
