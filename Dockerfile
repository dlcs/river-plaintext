FROM ubuntu
RUN apt-get update -y
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD /app/run_river_plaintext.sh