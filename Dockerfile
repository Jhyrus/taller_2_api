# 
FROM python:3.10

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install uvicorn
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN python -m spacy download es_core_news_sm

# 
COPY . .
WORKDIR /code/app
# 
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]