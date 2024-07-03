<div align="center">

<!-- logo -->
<img src="images/사용자행동분석_로그수집프로세스.png" alt="설명" style="width: 500px; height: auto;">

# 📌 사용자 행동 로그 분석 프로젝트

[<img src="https://img.shields.io/badge/-readme.md-important?style=flat&logo=google-chrome&logoColor=white" />]() [<img src="https://img.shields.io/badge/-tech blog-blue?style=flat&logo=google-chrome&logoColor=white" />]() [<img src="https://img.shields.io/badge/release-v0.0.0-yellow?style=flat&logo=google-chrome&logoColor=white" />]() 
<br/> [<img src="https://img.shields.io/badge/프로젝트 기간-2022.12.10~2022.12.19-green?style=flat&logo=&logoColor=white" />]()
</div> 

## 📜 프로젝트 개요
이 프로젝트는 사용자 행동 로그를 세션 단위로 분석 및 필터링하고, 다양한 방식으로 집계한 후 MySQL에 저장하는 플랫폼입니다. Scala와 Spark를 기반으로 구축되었으며, 대량의 로그 데이터를 효율적이고 안정적으 처리하도록 설계되었습니다

### **📌 주요 기능**

- 사용자 행동 로그 수집 및 처리
- 세션 단위의 데이터 분석
- Accumulator를 활용한 집계
- Spark 기반의 데이터 필터링 및 가공
- MySQL 저장 및 BI 연동

<br />


## 🛠️ 로그 수집 프레임워크
Spark Core를 사용하여 배치 처리 방식으로 로그를 수집합니다. 로그 수집 과정은 다음과 같습니다.

<br />

<img src="images/운영과정.png" alt="설명" style="width: 500px; height: auto;">

<br />

<img src="images/spark-history.png" alt="설명">

<br />

<img src="images/상세.png" alt="설명" style="width: 500px; height: auto;">

<br />


## 📋 요구사항 분석

<br />

**1️⃣ 데이터 수집**

- Hive / fs 사용자 로그를 가져와 Spark RDD로 변환
- 로그 형식: Parquet 파일

**2️⃣ 데이터 처리**

- 세션 단위 그룹핑 (session_id 기반)
- Accumulator를 활용하여 실시간 통계 계산
- 사용자 속성 및 행동 데이터 필터링

**3️⃣ 데이터 저장**

- MySQL에 통계 데이터를 저장
- BI 및 마케팅 시스템과 연동 가능하도록 설계

<br />


## 📂 프로젝트 구조 분석
<br />

      │── scala
      │   ├── analysis
      │   │   ├── session
      │   │   │   ├── accumulator
      │   │   │   ├── bean
      │   │   │   ├── Function
      │   │   │   ├── APP.scala
      │   ├── commons
      │   │   ├── conf
      │   │   ├── constant
      │   │   ├── model
      │   │   ├── pool
      │   │   ├── utils




<br />

### **📁 주요 디렉토리 설명**

**1️⃣ analysis (데이터 분석)**

- `session/APP.scala`: 세션 분석의 메인 실행 파일
- `session/accumulator/SessionAggrStatAccumulator.scala`: Spark Accumulator 활용한 통계 분석
- `session/bean/`: 세션 관련 데이터 모델
    - `SessionAggrStat.scala`: 세션 통계 결과 저장
    - `SessionDetail.scala`: 세션 상세 데이터 저장
    - `Top10Category.scala`: 인기 카테고리 정보 저장
- `session/Function/`: 데이터 처리 함수
    - `Demand1Function.scala`: 핵심 분석 로직
    - `UserSessionAnalysisFunction1.scala`: 사용자 세션 분석 실행

**2️⃣ commons (공통 설정 및 유틸리티)**

- `conf/ConfigurationManager.scala`: 설정 파일 로드
- `constant/Constants.scala`: 프로젝트 상수 저장
- `model/`: 데이터 모델
    - `ProductInfo.scala`: 제품 정보 저장
    - `UserInfo.scala`: 사용자 정보 저장
    - `UserVisitAction.scala`: 사용자 행동 데이터 저장
- `pool/`: MySQL 연결 풀
- `utils/`: 유틸리티 함수
    - `DateUtils.scala`: 날짜 변환
    - `NumberUtils.scala`: 숫자 변환
    - `ParamUtils.scala`: 매개변수 처리
    - `StringUtils.scala`: 문자열 유틸리티
    - `ValidUtils.scala`: 데이터 검증
 
## 🤔 기술적 이슈와 해결 과정

프로젝트 진행과정 & 문제해결 소개

https://heonyz.tistory.com/


<br />
