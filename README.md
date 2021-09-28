# What is [lambda-group](https://bitbucket.org/apartalk/hawkeye/src)?
* except for notification role of Apartalk
    * Hawkeye(푸쉬기능)을 제외한 람다 함수들 모음
* serverless service
* version 
    * python : 3.8
## Deploy
* build_{$name}.sh 실행 하여 생성된 lambda.zip을 해당하는 AWS Lambda Service로 업로드 한다.
* deploy 폴더에는 각 람다 함수가 들어가 있고 build_{$name}.sh 실행 시 업로드 형태로 패키징 해준다.
## A point of caution
* Lambda Service 특성상 필요 패키지는 root 경로에 둔다.
* boto3 등 AWS Lambda Service에서 기본적으로 제공해주는 패키지들은 lambda.zip에서 제거한다.(로컬 테스트용)
## Test
* Dev RDS, SNS arn setting 
* local_test.py 실행
## Trouble Shooting
### No module named 'psycopg2._psycopg'
- 아래 github에서 빌드된 psycopg2를 가져온다. (Hawkeye의 경우는 3.8)
- python pacakge 폴더를 생성 후 위에서 가져온 python file를 복사한다.
### RDS 접속 시 보안그룹
- RDS 보안그룹에 해당하는 VPC 그룹을 추가해줘야 한다.
## Todo-list
### 우선순위 Highest
* 람다의 실행최대시간 Test (15 Min) 

### 우선순위 Low
* S3 업로드를 통한 배포 자동화
* dirty package structure 정리 (Local)