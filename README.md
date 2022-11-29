# airflow_beta_pipeline
https://github.com/hufs0529/project_bigdata  ![치킨 222](https://user-images.githubusercontent.com/81501114/204549575-b8b6b3ad-63dd-4a3e-b506-371f140e6d94.png)

#### 앞선 '리뷰 자동 생성기'에서 다음과 같이 자동으로 리뷰를 생성하지만 리뷰의 상태가 사용자 마음에 들지 않을 수 있다
#### 날짜별로 마음에 들지 않는 리뷰 파일 생성 

![화면 캡처 2022-11-29 230500](https://user-images.githubusercontent.com/81501114/204550003-296ee74b-19bc-4549-840d-83a6c6967a54.png) 

> 다음과 같이 불만족스러운 리뷰가 있다고 가정
> > 위 csv파일을 자정마다 DB에 자동저장 및 HDFFS환경에서 Spark로 데이터 처리
> > > 나중에 이 데이터로 모델의 정확도를 높일 수 있게 데이터를 제공해줄 수 있는 모델에게 제공

## Airflow Scheduling
####  create_table  >> is_review_currencies_file_available >> process_review >> store_review >> saving_review 
####  saving_review >> creating_reviewtable >> review_processing

1. create_table - postgres db Table 생성
2. FileSensor - 파일 유무 확인
3. process_review - DB전송
4. store_review - DB 전송
5. saving_review - HDFS로 파일 전송
6. creating_reviewtable - Hive 테이블 생성
7. review_processing - Spark로 전처리
