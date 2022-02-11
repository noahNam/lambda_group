cp ./deploy/dau_collector.py ./lambda_function.py
zip -r ./deploy/dau_collector.zip ./requests ./psycopg2 ./certifi ./chardet ./idna ./lambda_function.py