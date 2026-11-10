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

## 실습 환경

Docker Compose에서 Airflow 스케줄러와 워커, Spark Standalone의 마스터와 워커를 실행했습니다.
AWS S3는 원본 CSV와 집계 결과를 저장하는 데 사용했습니다.

## 회고

Airflow에서 태스크 의존성을 정의하고 각 태스크의 성공 여부를 로그로 확인했습니다.
Spark로 CSV를 집계하고 Parquet 결과를 S3에 올리는 흐름을 실습했습니다.
