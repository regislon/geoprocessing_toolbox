FROM python:3.11-slim


WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends

RUN python3 -m venv /opt/venv

COPY pyproject.toml ./

COPY geoprocessing_toolbox/ /app

RUN pip install .[dev]




RUN python -m ipykernel install --user --name=venv 
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]


