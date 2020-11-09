FROM python:3.8
LABEL maintainer="Julian Gruendner <julian.gruendner@fau.de>"

WORKDIR /opt/refInt
COPY src /opt/refInt
RUN pip3 install -r requirements.txt
RUN pip3 install git+https://github.com/smart-on-fhir/client-py.git
RUN chmod +x startup.prod.sh

CMD ["bash", "startup.prod.sh"]