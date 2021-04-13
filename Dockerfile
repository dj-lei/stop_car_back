# ===============
# --- Release ---
# ===============
FROM python:3.7.9-buster
LABEL maintainer="ru_be"

RUN mkdir -p /ru_be

WORKDIR /ru_be
COPY ./ ./
RUN pip3 install -r requirements.txt

EXPOSE 8000
EXPOSE 9000

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]
